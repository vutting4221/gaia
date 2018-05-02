/* @flow */

import { StorageAuthentication as StorageAuth } from './StorageAuthentication'
import { ValidationError } from './errors'
import logger from 'winston'
import stream from 'stream'

function bufferStream(input: stream.Readable, maxLength: number) {
  return new Promise((resolve, reject) => {
    let buffers = []
    let length = 0
    input.on('error', reject)
    input.on('data', (data) => {
      if (length < maxLength) {
        buffers.push(data)
        length += data.length
      } else {
        if (data.length > 0) {
          reject(new ValidationError('Too much data uploaded'))
        }
      }
    })
    input.on('end', () => resolve(
      Buffer.concat(buffers)
        .slice(0, maxLength)))
  })
}

import type { Readable } from 'stream'
import type { DriverModel } from './driverModel'

export class HubServer {
  driver: DriverModel
  proofChecker: Object
  whitelist: Array<string>
  serverName: string
  constructor(driver: DriverModel, proofChecker: Object,
              config: { whitelist: Array<string>, servername: string }) {
    this.driver = driver
    this.proofChecker = proofChecker
    this.whitelist = config.whitelist
    this.serverName = config.servername
  }

  // throws exception on validation error
  //   otherwise returns void.
  validate(address: string, requestHeaders: { authorization: string }) {
    if (this.whitelist && !(this.whitelist.includes(address))) {
      throw new ValidationError('Address not authorized for writes')
    }

    let authObject = null
    try {
      authObject = StorageAuth.fromAuthHeader(requestHeaders.authorization, this.serverName)
    } catch (err) {
      logger.error(err)
    }

    if (!authObject) {
      throw new ValidationError('Failed to parse authentication header.')
    }

    authObject.isAuthenticationValid(address, true)
  }

  handleRequest(address: string, path: string,
                requestHeaders: {'content-type': string,
                                 'content-length': string,
                                 authorization: string},
                stream: Readable) {
    this.validate(address, requestHeaders)

    let contentType = requestHeaders['content-type']

    if (contentType === null || contentType === undefined) {
      contentType = 'application/octet-stream'
    }

    const writeCommand = { storageTopLevel: address,
                           path, stream, contentType,
                           contentLength: parseInt(requestHeaders['content-length']) }

    return this.proofChecker.checkProofs(address, path)
      .then(() => this.driver.performWrite(writeCommand))
  }

  putInboxMessage(destinationAddress: string,
                  senderAddress: string,
                  requestHeaders: {'content-type': string,
                                   'content-length': string,
                                   authorization: string},
                  stream: stream.Readable) {
    this.validate(senderAddress, requestHeaders)
    const contentLength = parseInt(requestHeaders['content-length']) > this.inboxItemSize ?
          this.inboxItemSize : parseInt(requestHeaders['content-length'])

    return bufferStream(stream)
      .then( messageBuffer => {
        return this.driver.getReadURLPrefix()
          .then(readUrlPrefix => {
            const inboxUrl = `${readUrlPrefix}/${destinationAddress}-inbox/`
          })
      })
  }
}
