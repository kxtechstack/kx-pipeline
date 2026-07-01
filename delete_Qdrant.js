require("dotenv").config();

const { QdrantClient } = require("@qdrant/js-client-rest");

const client = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const CLIENT_ID = "b61b4d3b-caeb-457b-9971-636c83688ee4";

async function deleteClientData() {
  try {
    console.log("Connecting to Qdrant...");
    console.log("URL:", process.env.QDRANT_URL);

    // Delete from policy_articles
    const result1 = await client.delete("policy_articles", {
      filter: {
        must: [
          {
            key: "client_id",
            match: {
              value: CLIENT_ID,
            },
          },
        ],
      },
    });

    console.log("Deleted from policy_articles:", result1);

    // Delete from dedup_titles
    const result2 = await client.delete("dedup_titles", {
      filter: {
        must: [
          {
            key: "client_id",
            match: {
              value: CLIENT_ID,
            },
          },
        ],
      },
    });

    console.log("Deleted from dedup_titles:", result2);

    console.log("Done!");
  } catch (err) {
    console.error("Error:", err);
  }
}

deleteClientData();