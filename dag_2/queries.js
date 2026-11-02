// Aggregations over the reviews load_reviews_to_mongo loads.
// Run all:  docker compose exec -T mongo mongosh --quiet airflow_data --eval "$(cat dags/dag_2/queries.js)"
// Or paste one pipeline array into the Aggregations tab of MongoDB Compass (airflow_data.reviews).
//
// `score` and `thumbsUpCount` are numbers and `at` is a BSON Date. Other fields are strings, with "-"
// where a value was missing or, for `content`, held no text. `repliedAt` is a Date, or "-" if there was
// no reply.

const PLACEHOLDER = "-";

// 1. Top 5 most frequent comments ("-" is not a comment, so it is left out).
const topComments = [
  { $match: { content: { $ne: PLACEHOLDER } } },
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1, _id: 1 } },
  { $limit: 5 },
  { $project: { _id: 0, content: "$_id", count: 1 } },
];

// 2. All entries whose content is shorter than 5 characters (counted in Unicode code points).
//    "-" means the review had no text, so it is left out, as in query 1.
const shortContent = [
  { $match: { content: { $ne: PLACEHOLDER }, $expr: { $lt: [{ $strLenCP: "$content" }, 5] } } },
];

// 3. Average rating per day; `day` is a BSON Date at midnight UTC.
const avgRatingPerDay = [
  { $group: { _id: { $dateTrunc: { date: "$at", unit: "day", timezone: "UTC" } }, avgRating: { $avg: "$score" } } },
  { $sort: { _id: 1 } },
  { $project: { _id: 0, day: "$_id", avgRating: 1 } },
];

const reviews = db.getCollection("reviews");

print("== 1. Top 5 comments");
printjson(reviews.aggregate(topComments).toArray());

print("== 2. Content shorter than 5 characters");
const short = reviews.aggregate(shortContent).toArray();
print(`${short.length} entries, first 5:`);
printjson(short.slice(0, 5));

print("== 3. Average rating per day");
const days = reviews.aggregate(avgRatingPerDay).toArray();
print(`${days.length} days, first and last 3:`);
printjson([...days.slice(0, 3), ...days.slice(-3)]);
