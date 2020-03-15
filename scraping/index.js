const puppeteer = require("puppeteer");
const download = require("image-downloader");
const path = require("path");
const fs = require('fs');
const {promisify} = require('util');
const {storage} = require('@google-cloud/storage');
exports.subscribe = pubsubMessage => {
  // Print out the data from Pub/Sub, to prove that it worked
  try {
    const scrapeImgUrls = async () => {
      try {
        const browser = await puppeteer.launch();
        const page = await browser.newPage();
        await page.goto("https://www.facebook.com/pg/KashuNamaadhuMV/photos");

        const photos = await page.evaluate(() => {
          const teamsRow = document.querySelectorAll("div[role=presentation]");
          const data = [];
          for (const tr of teamsRow) {
            data.push(tr.querySelector("a").getAttribute("href"));
          }

          return data;
        });
        const urls = [];
        var urlRegex =
          "^(?!mailto:)(?:(?:http|https|ftp)://)(?:\\S+(?::\\S*)?@)?(?:(?:(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}(?:\\.(?:[0-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))|(?:(?:[a-z\\u00a1-\\uffff0-9]+-?)*[a-z\\u00a1-\\uffff0-9]+)(?:\\.(?:[a-z\\u00a1-\\uffff0-9]+-?)*[a-z\\u00a1-\\uffff0-9]+)*(?:\\.(?:[a-z\\u00a1-\\uffff]{2,})))|localhost)(?::\\d{2,5})?(?:(/|\\?|#)[^\\s]*)?$";
        var url = new RegExp(urlRegex, "i");
        for (const photo of photos) {
          console.log(photo);
          if (photo.length < 2083 && url.test(photo)) {
            const page = await browser.newPage();
            await page.goto(photo);
            const im = await page.evaluate(() => {
              const teamsRow = document.querySelector("div.mtm");
              const image = teamsRow.querySelector("img.scaledImageFitWidth");
              const temp = image.getAttribute("src");
              return temp;
            });
            urls.push(im);
          }
        }
        // console.log(JSON.stringify(urls))
        return urls;
      } catch (e) {
        console.log(e);
        throw e;
      }
    };

    const downloadImg = async (options = {}) => {
      try {
        const { filename } = await download.image(options);
        const tempLocalPath = `/tmp/${path.basename(filename)}`;
        const bucketName = "processed_kashunamaadhu";
        // Upload result to a different bucket, to avoid re-triggering this function.
        const bucket = storage.bucket(bucketName);

        // Upload the Blurred image back into the bucket.
        const gcsPath = `gs://${bucketName}/${filename}`;
        try {
          await bucket.upload(tempLocalPath, { destination: filename });
          console.log(`Uploaded image to: ${gcsPath}`);
        } catch (err) {
          throw new Error(`Unable to upload image to ${gcsPath}: ${err}`);
        }

        // Delete the temporary file.
        const unlink = promisify(fs.unlink);
        return unlink(tempLocalPath);

        // console.log("⬇️  ", path.basename(filename)); // => image.jpg
      } catch (e) {
        throw e;
      }
    };

    const downloadAll = async () => {
      const imgs = await scrapeImgUrls();

      try {
        await Promise.all(
          imgs.map(async file => {
            await downloadImg({
              url: file,
              dest: "/tmp"
            });
          })
        );

        console.log(
          `👌  Done -- downloaded \x1b[36m${imgs.length}\x1b[0m death notice!`
        );
      } catch (e) {
        console.log(e);
        throw e;
      }
    };

    downloadAll();
  } catch (error) {
    console.log(error);
  }
};
