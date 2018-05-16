/* @flow */

import { StorageAuthentication as StorageAuth } from './StorageAuthentication'
import { ValidationError } from './errors'
import logger from 'winston'
import { bufferStream, stringToStream, sleep } from './utils'
import fetch from 'node-fetch'

import type { Readable } from 'stream'
import type { DriverModel } from './driverModel'

export class HubServer {
  driver: DriverModel
  proofChecker: Object
  whitelist: Array<string>
  serverName: string
  inboxItemSize: number
  constructor(driver: DriverModel, proofChecker: Object,
              config: { whitelist: Array<string>, servername: string }) {
    this.driver = driver
    this.proofChecker = proofChecker
    this.whitelist = config.whitelist
    this.serverName = config.servername
    this.inboxItemSize = 160
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

  confirmInboxWrite(inboxUrl: string, strInbox: string) {
    let confirmPromise = Promise.reject()
    for (let i = 0; i < 5; i++) {
      confirmPromise = confirmPromise
        .catch(() => sleep(500)
               .then(() => fetch(inboxUrl))
               .then(resp => resp.text())
               .then(outputText => {
                 if (outputText !== strInbox) {
                   throw new Error('Failed to propagate changes to inbox.')
                 }
               })
              )
    }

    return confirmPromise
  }

  putInboxMessage(destinationAddress: string,
                  senderAddress: string,
                  requestHeaders: {'content-type': string,
                                   'content-length': string,
                                   authorization: string},
                  stream: Readable) {
    this.validate(senderAddress, requestHeaders)
    const contentLength = parseInt(requestHeaders['content-length']) > this.inboxItemSize ?
          this.inboxItemSize : parseInt(requestHeaders['content-length'])
    const timeStamp = '00000'

    return bufferStream(stream, contentLength)
      .then( messageBuffer => {
        return this.driver.getReadURLPrefix()
          .then(readUrlPrefix => {
            const inboxUrl = `${readUrlPrefix}/${destinationAddress}-meta/inbox.json`
            // now we must lock!
            fetch(inboxUrl)
              .then(resp => resp.json())
              .then(currentInbox => {
                currentInbox.push({ message: messageBuffer.toString(),
                                    timeStamp,
                                    senderAddress })
                const strInbox = JSON.stringify(currentInbox)
                const inboxLength = Buffer.from(strInbox).length
                const stream = stringToStream(strInbox)
                return this.driver.performWrite({ storageTopLevel: `${destinationAddress}-meta`,
                                                  path: 'inbox.json',
                                                  stream,
                                                  contentType: 'application/json',
                                                  inboxLength })
                  .then(() => this.confirmInboxWrite(inboxUrl, strInbox))
              })
          })
      })
  }
}
