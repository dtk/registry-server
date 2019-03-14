import {connect, NatsConnectionOptions, Payload, Client} from 'ts-nats';
import * as protobuf from 'protobufjs';
// import * as flatfile from 'flat-file-db';
var flatfile = require('flat-file-db');

const PROTO_MESSAGE_PATH = 'data/registryServer.proto';

export default class RegistryService {
  public nc: any;

  constructor(url?: string) {
    connect({servers: ["nats://dev22.r8network.com:4222"], payload: Payload.BINARY}).then((nc) => {
      this.nc = nc;
      this.listenForRegistrations();
      this.listenForRequests();
    }).catch((ex) => {
      console.log("Unable to connect to nats server");
    });
  }

  async getRegisteredNode(guid: string) {
    var db = flatfile.sync('data/registeredExecutors');
    let a = db.get(guid);
    
  }

  private async listenForRegistrations() {
    console.log("Started listening for registration messages");
    if(this.nc != undefined) {
      let sub = await this.nc.subscribe('registration_service', async (err: any, msg: any) => {
        if(err) {
          throw new Error(err.message);
        } else {
          let decodedMessage = await this.decodeRegistrationMessage(msg.data);

          let msgContent = JSON.parse(JSON.stringify(decodedMessage));

          console.log(msgContent);

          var db = flatfile.sync('data/registeredExecutors');

          let tags = JSON.stringify(msgContent['tags']);
          db.put(msgContent['guid'], { tags: msgContent['tags'] });

          db.close();
        }
      });
    } else {
      console.log('Cannot start listener because registry service is not connected to nats');
    }
  }

  private async listenForRequests() {
    console.log("Started listening for requests");
    if(this.nc != undefined) {
      let sub = await this.nc.subscribe('query_registration_service', async (err: any, msg: any) => {
        if(err) {
          throw new Error(err.message);
        } else {
          let tags = msg.data;
          for (let tag of tags) {
            console.log(tag);
          }
        }
      });
    } else {
      console.log('Cannot start listener because registry service is not connected to nats');
    }
  }

  async decodeRegistrationMessage(message: Buffer) {
    return await protobuf.load(PROTO_MESSAGE_PATH).then(async function(root) {
      var executableActionMessage = root.lookupType("registryserverpackage.RegistrationMessage");

      var decodedMessage = executableActionMessage.decode(message);

      console.log("DECODED MESSAGE:")
      console.log(decodedMessage);

      return decodedMessage;
    }).catch((ex) => {
      console.log(ex.message);
    });
  }

  public async getStreamBuffer(stream: any) {
    return await protobuf.load(PROTO_MESSAGE_PATH).then(function(root) {
      var registrationMessage = root.lookupType("registryserverpackage.RegistrationMessage");

      var errMsg = registrationMessage.verify(stream);
      if (errMsg)
          throw Error(errMsg);

      var message = registrationMessage.create(stream);
      var buffer = new Buffer(registrationMessage.encode(message).finish());

      return buffer;
    }).catch(reason => {
      console.log("Promise rejected because of:");
      console.log(reason);
      throw Error(reason);
    });
  }
}