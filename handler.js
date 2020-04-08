var AWS = require('aws-sdk');
var s3 = new AWS.S3();
let Client = require('ssh2-sftp-client');
let sftp = new Client();
const path = require('path');
const fs = require('fs');
const pg = require('pg');
var zlib = require('zlib');
const stream = require('stream');
var glue = new AWS.Glue();

let SFTP_CONFIG = {
    host: '10.55.80.1',
    port: '10022',
    username: 'ivecloud',
    password: 'G3Q$9bT2'
};



let DATABASE_CONFIG = {
    user: process.env.db_user || 'ivesbdbadmin',
    password: process.env.db_pass || 'ivesb$db9admin',
    database: process.env.db_name || 'IVEDB',
    host: process.env.db_host || 'ive-sandbox-db.cluster-ce6ethc5pzux.us-west-2.rds.amazonaws.com',
    port: process.env.db_port || 5432
};




let SFTP_FOLDER = '/Inbox';
const folder = '/tmp/temp.txt';

const uploadStream = ({ Bucket, Key }) => {
    const s3 = new AWS.S3();
    const pass = new stream.PassThrough();
    console.log('----------pass---------')
    console.log(pass)
    console.log(JSON.stringify({ Bucket, Key }, null, 2))
    return {
        writeStream: pass,
        promise: s3.upload({ Bucket, Key, Body: pass }).promise(),
    };
}






/** Start the ETL glue job to insert data to vehicle_data table 
 * Source data catalog table name is sent as a parameter to the glue job */

function callGlueJob(filename) {
    console.log("calling Glueeeeeeeeeeeeee")
    return new Promise(async(resolve, reject) => {
        try {

            console.log("call glue job" + filename);
            var params = {
                // JobName: '',
                // Arguments: {
                // '--sourceFile': filename,
                // '--glueRegion': 'us-west-2',
                // '--crawlerName1': 'ive-dev-datamart-fixedlength-file-crawler1',
                // '--crawlerName2': 'ive-dev-datamart-fixedlength-file-crawler2',
                // '--glueDb': 'inputdb',
                // }
                JobName: 'ive_dev_partssummary',
                Arguments: {
                    '--sourceFile': filename,
                    '--glueRegion': 'us-west-2',
                    '--crawlerName1': 'ive-dev-datamart-fixedlength-file-crawler1',
                    '--crawlerName2': 'ive-dev-datamart-fixedlength-file-crawler2',
                    '--glueDb': 'inputdb',
                }
            };


            console.log('glue params')
            console.log(params)
            //Invoke job run
            glue.startJobRun(params, async(err, data) => {
                if (err) reject(err)
                else
                    console.log(data); // successful response
                var jobId = data['JobRunId'];
                console.log('jobId ' + jobId);

                var params = {
                    // JobName: 'ive-dev-datamart-loader-1',
                    JobName: 'ive_dev_partssummary',
                    /* required */
                    RunId: jobId,
                    /* required */
                };
                var status = await glue.getJobRun(params, function(err, data) {
                    if (err) console.log(err, err.stack); // an error occurred
                    else console.log(data); // successful response
                }).promise();
                console.log('status ' + status);
                resolve(status);

            });

        }
        catch (err) {
            console.log(err);
            reject(err);
        }
    })


}




const removeExtention = (filename) => {
    // get the last dot
    const fileNamelengthWithoutExten = filename.split('.').length - 1;
    const filenameinarray = filename.split('.');
    let name = ''
    for (let i = 0; i < fileNamelengthWithoutExten; i++) {
        name += filenameinarray[i]
    }
    return name
}


const splitfileNameAndExtension = (filename) => {
    console.log(filename, 'filename')
    // get the last dot
    const fileNamelengthWithoutExten = filename.split('.').length - 1;
    const filenameinarray = filename.split('.');
    let extension = filename.substring(filename.lastIndexOf('.') + 1, filename.length) != '.txt' ? '' : filename.substring(filename.lastIndexOf('.') + 1, filename.length)
    let name = extension.length > 0 ? filename.replace('.' + extension, '') : filename
    // for (let i = 0; i < fileNamelengthWithoutExten; i++) {
    //     name += filenameinarray[i]
    // }

    return {
        name,
        extension
    }
}


exports.handler = async function(event, context, callback) {
    try {
        const gofile = () => new Promise(async(res, rej) => {
            await sftp.connect(SFTP_CONFIG)
                // .then(() => sftp.list('/Inbox/TestResponse'))
                .then(async(chunk) => new Promise(async(res, rej) => {
                    console.log('here')
                    // let fileWtr = fs.createWriteStream('/tmp/sample.txt.gz');

                    // fileWtr.on('open', function(fd){
                    //     console.log((fd))
                    // })

                    // let fileWtr = fs.createWriteStream(folder);


                    // readStream.on('open', function() {
                    //     // This just pipes the read stream to the response object (which goes to the client)
                    //     const { writeStream, promise } = uploadStream({ Bucket: 'ive-dev-parts-info', Key: `uploadingSample` });
                    //     readStream.pipe(writeStream);
                    //     promise.then(e => {
                    //         res(e)
                    //     }).catch(e => rej(e))

                    // });

                    // readStream.on('error', function(err) {
                    //     rej(err);
                    // });
                    // const { writeStream, promise } = uploadStream({ Bucket: 'ive-dev-parts-info', Key: `uploadingSample` });




                    // promise.catch(e => rej(e))
                    function upload() {
                        console.log(arguments)
                    }

                    // let out = fs.createWriteStream(folder, {
                    //     flags: 'w',
                    //     encoding: null
                    // });
                 
                        
                
      
                    await sftp.get('/Inbox/TestResponse/GBE3BKZ1.DBK5Z01D.X0000001', out).catch(e => console.log("catch", e))
                    sftp.end()
                }))
                .catch(e => rej('ddada'))
        })

        let res_go = await gofile();

        console.log('here file will be moved', res_go)
        return res_go;


        console.time('---Job Processig Inintiated---')
        console.log('even logging')
        console.log(event)

        /*
            ## This function wil take 
            ## glue params 
            ## s3 params
            ## sftp params
            ## params for deletion operation flag on s3 and sftp optionaly 
            
        */



        const filename = event['filename'] || 'Parts_Info_TMMK.txt'
        const S3targets3bucket = event['target_s3_bucket'] || 'ive-dev-parts-info'
        const sftpsourceFolder = event['sftp_source_Folder'] || SFTP_FOLDER
        let shouldDeletefromdestination = event['shouldDeletefromdestination'] || true
        let TriggerGlue = event['TriggerGlue'] || true
        let glueParams = event['glueParams'] || false



        // ## could be altered based on the params or arugs from input
        const S3DestFilename = S3targets3bucket




        const fileNameAndExtension = await splitfileNameAndExtension(filename)
        console.log(fileNameAndExtension, 'fileNameAndExtension')

        console.log('deleting the file')
        const deleteFileFromSftp = () => new Promise(async(res, rej) => {
            await sftp.connect(SFTP_CONFIG)
                .then(() => {
                    sftp.delete(`${sftpsourceFolder}/${filename}`).then(() => {
                        console.log("File deleted from sftp " + `${SFTP_FOLDER}/${filename}`)
                        sftp.end();
                    }).catch((err) => {
                        rej(err)
                        console.log("error while deleting file from sftp " + `${SFTP_FOLDER}/${filename}`)
                        sftp.end();

                    });

                })
                .catch(() => {})
        })




        const write = file => {
            return new Promise((res, rej) => {
                fs.writeFile(file, JSON.stringify({ message: '' }), async(err) => {
                    if (err) {
                        return rej(err)
                    }

                    let option = {
                        concurrency: 640, // integer. Number of concurrent reads to use
                        chunkSize: 32768, // integer. Size of each read in bytes
                        // step: function(total_transferred, chunk, total) {
                        //     // console.log(total_trans+ferred, chunk, total)
                        // }
                    }
                    console.log('------prior to sftp connection----')
                    console.log(`${sftpsourceFolder}/${filename}`)

                    await sftp.connect(SFTP_CONFIG)
                        .then(() => sftp.get(`${sftpsourceFolder}/${filename}`))
                        .then(async(chunk) => {
                            sftp.end()
                            console.log('here file will be moved')


                            let promise = s3.upload({ Bucket: S3targets3bucket, Key: filename, Body: chunk }).promise();
                            console.log('here file is moved')
                            console.log('------then block sftp connection----')
                            // const { writeStream, promise } = uploadStream({ Bucket: S3targets3bucket, Key: `${filename}` });
                            // writeStream.end(chunk)
                            // const readStream = writeStream.end(chunk)
                            // readStream.pipe(writeStream);
                            promise
                                .then((e) => {
                                    res(e)
                                    console.log('------error in moving file----')
                                    console.log('------Promise block sftp connection----')
                                })
                                .catch(er => rej(er))

                        })
                        .catch(err => {
                            console.log('error--')
                            console.log('------then block sftp connection----')
                            console.log(err)
                        })
                })
            })
        }


        return write(folder).then(async res => {
            console.log('------return block after write----')
            console.log(shouldDeletefromdestination + 'shouldDeletefromdestination')
            //  if (shouldDeletefromdestination) {
            //        deleteFileFromSftp()
            //   }
            console.log(TriggerGlue + 'TriggerGlue')
            const filenameforglue = fileNameAndExtension.name + '_' + fileNameAndExtension.extension
            //  const statusofS3 = TriggerGlue ? await callGlueJob(filenameforglue) : null
            return statusofS3
        }).catch(err => {
            console.log(('error while s3 write process flow'))
            throw new Error(err)
        })


    }
    catch (e) {

        return e
    }
}
