/**
 * better to use pipeline than pipe
 */

import fs from "fs";
import csv from "csvtojson";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import { createGunzip } from "zlib";
import mongoose from "mongoose";
import bufferingObjectStream from "buffering-object-stream";

import UserInfoModel from "./db/user.js";

// use pipe to solve back pressure
const main = async () => {
  // save data to database
  await mongoose.connect(
    "mongodb+srv://?@cluster0.qidj8mr.mongodb.net/yourdb?retryWrites=true&w=majority"
  );

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

      // console.log("User: ", user);

      callback(null, user); // user
    },
  });

  const convertToNdJson = new Transform({
    objectMode: true,
    transform(user, enc, callback) {
      const ndjson = JSON.stringify(user) + "\n";
      callback(null, ndjson);
    },
  });

  const saveUser = new Transform({
    objectMode: true,
    async transform(user, enc, callback) {
      await UserInfoModel.create(user);
      callback(null);
    },
  });

  const saveUsers = new Transform({
    objectMode: true,
    async transform(users, enc, callback) {
      // const promises = users.map((user) => UserInfoModel.create(user)); // bad performance
      // await Promise.all(promises);

      // 往数据库中批量添加数据
      await UserInfoModel.bulkWrite(
        // good performance
        users.map((user) => ({
          insertOne: { document: user },
        }))
      );

      callback(null);
    },
  });

  console.time("savedb");

  try {
    await pipeline(
      readStream,
      csv({ delimiter: ";" }, { objectMode: true }),
      myTransform,
      myFilter,

      // saveUser, // bad performance
      bufferingObjectStream(200), // good performance need to choose right bufferSize
      saveUsers

      /**
       * 存储到数据库
       * saveUser
       */

      /**
       * 存储为json文件      
      convertToNdJson
      fs.createWriteStream("./data/export.ndjson")
       */

      /**
       * 存储为json文件, 并压缩
      convertToNdJson
      createGunzip, // 压缩
      fs.createWriteStream("./data/export.gz")
       */
    );

    console.log("Stream ended");

    console.timeEnd("savedb");

    process.exit(0);
  } catch (error) {
    console.error("Stream ended with error: ", error);
  }
};

main();
