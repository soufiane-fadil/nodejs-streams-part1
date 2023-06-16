const fs = require('fs')
const { faker } = require('@faker-js/faker')

const writeStream = fs.createWriteStream('./data/import.csv')

writeStream.write('name;email;age;salary;isActive\n')

for (let index = 0; index < 10000; index++) {
  const firstName = faker.person.firstName();
  const email = faker.internet.email({ firstName });
  const age = faker.number.int({ min: 10, max: 100 });
  const salary = faker.string.numeric({ length: 4, allowLeadingZeros: true });
  const active = faker.datatype.boolean();

  const arr = [
    firstName,
    email,
    age,
    salary,
    active
  ]
  writeStream.write(arr.join(';') + '\n')
}

writeStream.end();
