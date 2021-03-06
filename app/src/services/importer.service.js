const logger = require('logger');
const ConverterFactory = require('services/converters/converterFactory');
const _ = require('lodash');
const dataQueueService = require('services/data-queue.service');
const statusQueueService = require('services/status-queue.service');
const StamperyService = require('services/stamperyService');
const CONTAIN_SPACES = /\s/g;
const IS_NUMBER = /^\d+$/;
function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    }
    return false;
}

function convertPointToGeoJSON(lat, long) {
    return {
        type: 'point',
        coordinates: [
            long,
            lat
        ]
    };
}

class ImporterService {

    constructor(msg) {
        this.body = [];
        this.datasetId = msg.datasetId;
        this.provider = msg.provider;
        this.url = msg.fileUrl;
        this.dataPath = msg.dataPath;
        this.verify = msg.verified;
        this.legend = msg.legend;
        this.taskId = msg.taskId;
        this.index = msg.index;
        this.rowNumber = 0;
        this.type = msg.indexType;
        this.indexObj = {
            index: {
                _index: msg.index,
                _type: msg.indexType
            }
        };
        this.numPacks = 0;
    }

    async start() {
        return new Promise(async(resolve, reject) => {
            try {
                logger.debug('Starting read file');
                this.rowNumber = 0;
                const converter = ConverterFactory.getInstance(this.provider, this.url, this.dataPath, this.verify);
                // StamperyService
                if (this.verify) {
                    const blockchain = await StamperyService.stamp(this.datasetId, converter.sha256, converter.filePath, this.type);
                    statusQueueService.sendBlockChainGenerated(this.taskId, blockchain);
                }
                await converter.init();
                const stream = await converter.serialize();
                logger.debug('Starting process file');
                stream.on('data', this.processRow.bind(this, stream, reject));
                stream.on('error', (err) => {
                    logger.error('Error reading file', err);
                    reject(err);
                });
                stream.on('end', () => {
                    logger.debug(`Body data: ${this.body.length}`)
                    if (this.numPacks === 0 && this.body && this.body.length === 0) {
                        statusQueueService.sendErrorMessage(this.taskId, 'File empty');
                        resolve();
                        return;
                    }
                    logger.debug('Finishing reading file');
                    if (this.body && this.body.length > 0) {
                        this.checkColumnDataTypes(50);
                        // send last rows to data queue                        
                        dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                            this.body = [];
                            logger.debug('Pack saved successfully, num:', ++this.numPacks);
                            resolve();
                        }, function (err) {
                            logger.error('Error saving ', err);
                            reject(err);
                        });
                    } else {
                        resolve();
                    }
                });
            } catch (err) {
                logger.error(err);
                reject(err);
            }
        });
    }
    //this function will loop numRows into the body data to verify the first row contains all the correct data for the column data type
    //for now we are just checking for numbers, since that seems to be the cause for most data skips
    checkColumnDataTypes(numRows) {
        //every other row is an index row - so multiply by two
        const loopRows = numRows*2 > this.body.length ? this.body.length : numRows * 2;
        let colDataTypes = [];
        let dataKeys = Object.keys(this.body[1]); 
        for(var i = 1; i<(loopRows-1);i=i+2) {
            const row = this.body[i];
            _.forEach(dataKeys, function(rowKey) {
                if(IS_NUMBER.test(row[rowKey])) {
                    if(!colDataTypes[rowKey])
                        colDataTypes.push({key:rowKey,value:"NUMBER"})
                }
                else if(typeof row[rowKey] != 'undefined' && row[rowKey].length > 0) {
                    //let's delete the key so we no longer need to iterate through that key since there's no
                    //data serialization that needs to occur
                    //string should overwrite number
                    if(colDataTypes[rowKey] && colDataTypes[rowKey] === "NUMBER")
                        colDataTypes[rowKey] === "STRING"
                    else if(!colDataTypes[rowKey])
                        colDataTypes.push({key:rowKey,value:"STRING"})
                }
            })
        }

        let updateRows = 40 > this.body.length ? this.body.length : 40;
        //colDataTypes contains all keys that should be numbers.  Make sure the first row in body if empty has a zero in place to ensure the correct data type.
        for(var i = 1; i<(updateRows-1);i=i+2) {
            colDataTypes.forEach(item => {
                if(this.body[i][item.key] == null || (typeof this.body[i][item.key] === "string" && this.body[i][item.key].length === 0) ) {
                    if(item.value === "NUMBER")
                        this.body[i][item.key] = 0;
                    else if (item.value === "STRING")
                        this.body[i][item.key] = " ";
                }
            })
            }
    }
    async processRow(stream, reject, data) {
        try {
            stream.pause();
            try {
                if (_.isPlainObject(data)) {

                    _.forEach(data, function (value, key) {
                        
                        let newKey = key;
                        try {
                            if (newKey !== '_id') {
                                if (CONTAIN_SPACES.test(key)) {
                                    delete data[key];
                                    newKey = key.replace(CONTAIN_SPACES, '_');
                                }
                                if (IS_NUMBER.test(newKey)) {
                                    if (data[newKey]) {
                                        delete data[key];
                                    }
                                    newKey = `col_${newKey}`;
                                }
                                if(newKey === "") {
                                    delete data[key];
                                }
                                else if (!(value instanceof Object) && isJSONObject(value)) {
                                    try {
                                        data[newKey] = JSON.parse(value);
                                    } catch (e) {
                                        data[newKey] = value;
                                    }
                                } else if (!isNaN(value) && value != '') {
                                    data[newKey] = Number(value);
                                } else {
                                    data[newKey] = value;
                                }
                            } else {
                                delete data[newKey];
                            }
                        } catch (e) {
                            logger.error(e);
                            throw new Error(e);
                        }
                    });
                    
                    if (this.legend && (this.legend.lat || this.legend.long)) {
                        if (data[this.legend.lat] && data[this.legend.long]) {
                            data.the_geom = convertPointToGeoJSON(data[this.legend.lat], data[this.legend.long]);
                            data.the_geom_point = {
                                lat: data[this.legend.lat],
                                lon: data[this.legend.long]
                            };
                        }
                    }
                    logger.trace('Adding new row');
                    if(data.country && data.country.indexOf('#') > -1){
                        logger.debug('skip row - has hashes')
                    } else {
                        data["row_num"] = this.rowNumber;
                        this.rowNumber++;
                        this.body.push(this.indexObj);
                        this.body.push(data);    
                    }

                } else {
                    logger.error('Data and/or options have no headers specified');
                }
            } catch (e) {
                // continue
                logger.error('Error generating', e);
            }

            if (this.body && this.body.length >= 40000) {
                logger.debug('Sending data');
                logger.debug(`Body data 40000: ${this.body.length}`)

                dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                    this.body = [];
                    stream.resume();
                    logger.debug('Pack saved successfully, num:', ++this.numPacks);
                }, function (err) {
                    logger.error('Error saving ', err);
                    stream.end();
                    reject(err);
                });
            } else {
                stream.resume();
            }
        } catch (err) {
            logger.error('Error saving', err);
        }
    }

}

module.exports = ImporterService;
