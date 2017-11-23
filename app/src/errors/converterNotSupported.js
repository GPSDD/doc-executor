'use strict';
class ConverterNotSupported extends Error {
    constructor(status, message){
        super(message);
        this.status = status;
    }
}

module.exports = ConverterNotSupported;