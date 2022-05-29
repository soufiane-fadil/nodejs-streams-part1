
const fs = require('fs')
const csv = require('csvtojson')

const main = async () => {

  const readStream = fs.createReadStream('./data/import.csv')
    .pipe(csv({ delimiter: ';' }, { objectMode: true }))
    .on('data', (data) => {
      console.log('>>> Data');
      console.log(data);
    }).on('end', () => {
      console.log('Stream Ended');
    })

}

main()
