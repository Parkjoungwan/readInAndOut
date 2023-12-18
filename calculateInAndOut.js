const fs = require('fs');
const readline = require('readline');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { DateTime } = require('luxon');

const inputFile = 'input.csv';
const outputFile = 'output.csv';

const userEntries = {};

function detectTimeFormat(timeString) {
  // 정규 표현식을 사용하여 시간 형식을 판단
  const format1 = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/; // yyyy-MM-dd HH:mm:ss
  const format2 = /^\d{4}-\d{2}-\d{2} \d{1}:\d{2}:\d{2}$/; // yyyy-MM-dd H:mm:ss
  const format3 = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/; // yyyy-MM-dd HH:mm
  const format4 = /^\d{4}-\d{2}-\d{2} \d{1}:\d{2}$/; // yyyy-MM-dd H:mm
  
  if (format1.test(timeString)) {
    return 'yyyy-MM-dd HH:mm:ss';
  } else if (format2.test(timeString)) {
    return 'yyyy-MM-dd H:mm:ss';
  } else if (format3.test(timeString)) {
    return 'yyyy-MM-dd HH:mm';
  } else if (format4.test(timeString)) {
    return 'yyyy-MM-dd H:mm';
  } else {
    return 'Unknown Format';
  }
}

const makeOutput = () => {
    fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
        const deviceInfo = row['장치'];
        const name = row['사용자'];
        const logTimeString = row['날짜']

        // "OUT"을 찾아서 입장(IN) 또는 퇴장(OUT) 판단
        const isOut = deviceInfo.includes('OUT');
        const formatForTime = detectTimeFormat(logTimeString);
        if (formatForTime == 'Unknown Format'){
            console.log(row);
            console.log(time);
            console.log("FormatError!");
            process.exit();
        }
        const logTime = DateTime.fromFormat(logTimeString, formatForTime);
        
        if (!userEntries[name]) {
        userEntries[name] = [];
        }

        if (isOut) {
            userEntries[name].push({outTime: logTime});
        } else {
            userEntries[name].push({inTime: logTime});
        }
    })
    .on('end', () => {
        // 사용자별 입출입 기록을 시간순으로 정렬
        for (const name in userEntries) {
            userEntries[name].sort((a, b) => {
                let aTime;
                let bTime;
                if (a.inTime != undefined)
                    aTime = a.inTime;
                else if (a.inTime == undefined)
                    aTime = a.outTime;
                if (b.inTime != undefined)
                    bTime = b.inTime;
                else if (b.inTime == undefined)
                    bTime = b.outTime;
                return aTime - bTime;
            });
        }

        // 출력 CSV 파일 생성
        const csvWriter = createCsvWriter({
            path: outputFile,
            header: [
                { id: '이름', title: '이름' },
                { id: '입', title: '입' },
                { id: '출', title: '출' },
                { id: '근무시간', title: '근무시간' },
            ],
        });

        const records = [];

        for (const name in userEntries) {
            const entries = userEntries[name];
            for (let i = 0; i < entries.length; i += 2) {
                if (i + 1 < entries.length) {
                    if (entries[i + 1].inTime != undefined || entries[i].inTime == undefined) {
                        if (entries[i].inTime == undefined && entries[i + 1].inTime == undefined)
                            while(entries[i].inTime == undefined)
                                i++;
                        while (i + 1 < entries.length && entries[i + 1].inTime != undefined)
                            i++;
                    }
                    const inTime = entries[i].inTime;
                    const outTime = entries[i + 1].outTime;
                    if (i + 2 < entries.length && entries[i + 2].outTime != undefined) {
                        while (i + 2 < entries.length && entries[i + 2].outTime != undefined)
                            i++;
                    }
                    const duration = outTime.diff(inTime, ['hours', 'minutes', 'seconds']).toObject();

                    records.push({
                        이름: name,
                        입: inTime.toFormat('HH:mm:ss'),
                        출: outTime.toFormat('HH:mm:ss'),
                        근무시간: `${String(Math.floor(duration.hours)).padStart(2, '0')}:${String(Math.floor(duration.minutes)).padStart(2, '0')}:${String(Math.floor(duration.seconds)).padStart(2, '0')}`,
                    });
                }
            }
        }

        csvWriter
        .writeRecords(records)
        .then(() => {
            console.log('근무 시간 계산이 완료되었습니다.');
            groupDataByUser(outputFile); // makeOutput이 끝나면 groupDataByUser 호출
        })
        .catch((err) => {
            console.error('CSV 파일을 생성하는 도중 오류가 발생했습니다.', err);
        });
    });
}

function groupDataByUser(inputFile) {
    if (fs.existsSync(inputFile)) { // 파일이 존재하는지 확인
        const userEntries = {};
    
        const readStream = readline.createInterface({
            input: fs.createReadStream(inputFile),
            output: process.stdout,
            terminal: false
        });
    
        readStream.on('line', (line) => {
            const [user, ...data] = line.split(',');
            if (!userEntries[user]) {
                userEntries[user] = [];
            }
            userEntries[user].push(data.join(','));
        });
    
        readStream.on('close', () => {
            writeGroupedData(userEntries);
        });
    } else {
        console.error(`${inputFile} 파일이 존재하지 않습니다.`);
    }
}

function writeGroupedData(userEntries) {
    const outputStream = fs.createWriteStream("formattedOutPut.csv");

    for (const user in userEntries) {
        const data = userEntries[user];
        while (data.length > 0) {
            const chunk = data.splice(0, 5).join(',');
            outputStream.write(`${user},${chunk}\n`);
        }
    }

    outputStream.end(() => {
        console.log('데이터 그룹화 및 파일 작성이 완료되었습니다.');
    });
}

const main = async() => {
    await makeOutput();
}

main();
