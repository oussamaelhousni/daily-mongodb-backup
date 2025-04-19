const dotenv = require("dotenv");
const { MongoClient } = require("mongodb");
const path = require("path");
const fs = require("fs");
const AdmZip = require("adm-zip");
const cron = require("node-cron");

dotenv.config();

const DB_URI = process.env.DB_URI;
const DB_NAME = process.env.DB_NAME;
const BACKUP_DIR = path.join(__dirname, "backups", "popcard");
const TEMP_FOLDER = path.join(__dirname, "tmp");
const LOG_FILE = path.join(__dirname, "backup_log.txt");

if (!fs.existsSync(BACKUP_DIR)) {
  fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

if (!fs.existsSync(TEMP_FOLDER)) {
  fs.mkdirSync(TEMP_FOLDER, { recursive: true });
}

async function clearTempFolder() {
  try {
    const files = await fs.promises.readdir(TEMP_FOLDER);
    for (const file of files) {
      try {
        await fs.promises.unlink(path.join(TEMP_FOLDER, file));
      } catch (error) {}
    }
  } catch (error) {
    console.error(`Error clearing folder: ${error.message}`);
  }
}

async function backDbAsZip() {
  try {
    const client = await MongoClient.connect(DB_URI);
    const db = client.db(DB_NAME);

    // Get list of collections
    const collections = await db.listCollections().toArray();

    // List of paths to JSON files
    const jsonsPath = [];
    const zip = new AdmZip();

    // Write collections to temporary folder using streams
    for (const collection of collections) {
      const collectionName = collection.name;
      const outputFile = path.join(TEMP_FOLDER, `${collectionName}.json`);

      jsonsPath.push(outputFile);

      // Stream data from MongoDB and write to a file
      await new Promise((resolve, reject) => {
        const cursor = db.collection(collectionName).find({}).stream();
        const writeStream = fs.createWriteStream(outputFile);

        // Write opening bracket for JSON array
        writeStream.write("[");

        let isFirst = true;

        cursor.on("data", (doc) => {
          const jsonData = JSON.stringify(doc);

          if (isFirst) {
            writeStream.write(jsonData);
            isFirst = false;
          } else {
            writeStream.write("," + jsonData);
          }
        });

        cursor.on("end", () => {
          // Write closing bracket and close the stream
          writeStream.write("]");
          writeStream.end();
          resolve();
        });

        cursor.on("error", (err) => {
          writeStream.end();
          reject(err);
        });
      });
    }

    // Add JSON files to ZIP
    jsonsPath.forEach((filePath) => {
      zip.addLocalFile(filePath);
    });

    // Generate ZIP file name based on today's date
    const today = new Date();
    const zipPath = path.join(
      BACKUP_DIR,
      `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(
        2,
        "0"
      )}-${String(today.getDate()).padStart(2, "0")}.zip`
    );

    // Write ZIP to the backup directory and clear temporary folder
    zip.writeZip(zipPath);
    await clearTempFolder();

    const timestamp = new Date().toISOString();
    const finalLogMessage = `[${timestamp}] Backup completed successfully. ZIP saved to: ${zipPath}\n`;
    await fs.promises.appendFile(LOG_FILE, finalLogMessage);
  } catch (error) {
    console.error("Error during backup:", error);
  }
}

// Schedule the task to run every day at 23:00 UTC
cron.schedule("00 23 * * *", () => {
  backDbAsZip();
});
backDbAsZip();
