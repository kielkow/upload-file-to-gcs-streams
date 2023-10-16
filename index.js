const fs = require('node:fs');
const csv = require('csv-parser');
const { Storage } = require('@google-cloud/storage');

const storage = new Storage({
  credentials: require('./credentials.json'),
});

async function uploadFile() {
  const filename = 'file.csv';
  const bucketName = 'buildv1-om3scm';
  const destination = 'raw/teste/file.csv';

  const bucket = storage.bucket(bucketName);
  const file = bucket.file(destination);

  const stream = file.createWriteStream({
    metadata: {
      contentType: 'text/csv',
    },
  });

  const readStream = fs.createReadStream(filename);

  readStream
    .pipe(csv())
    .on('data', (row) => {
      console.log(row);
      stream.write(`${JSON.stringify(row)}\n`);
    })
    .on('end', () => {
      stream.end();
      console.log(`${filename} uploaded to ${bucketName}/${destination}.`);
    });

  return new Promise((resolve, reject) => {
    stream.on('error', reject);
    stream.on('finish', resolve);
  });
}

uploadFile().catch(console.error);
