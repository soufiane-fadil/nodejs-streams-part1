import fs from "fs";
import csv from "csvtojson";
import { Transform } from "stream";

// use pipe to solve back pressure
const main = async () => {
  const readStream = fs.createReadStream("./data/import.csv", {
    highWaterMark: 100,
  });

  const writeStream = fs.createWriteStream("./data/export.csv");

  const myTransform = new Transform({
    objectMode: true,
    transform(chunk, enc, callback) {
      const user = {
        name: chunk.name,
        email: chunk.email.toLowerCase(),
        age: Number(chunk.age),
        salary: Number(chunk.salary),
        isActive: chunk.isActive === "true",
      };
      callback(null, user); // return transform data: user
    },
  });

  const myFilter = new Transform({
    objectMode: true,
    transform(user, enc, callback) {
      if (!user.isActive) {
        callback(null);
        return;
      }

      callback(null, user);
    },
  });

  readStream
    .pipe(csv({ delimiter: ";" }, { objectMode: true }))
    .pipe(myTransform)
    .pipe(myFilter)
    .on("data", (data) => {
      console.log(">>> data: ");
      console.log(data);
    })
    .on("error", (err) => {
      console.error("Stream error: ", err);
    })
    .on("end", () => {
      console.log("Stream ended");
    });

  // writeStream.on("finish", () => {
  //   console.log("Write stream finished");
  // });
};

main();
