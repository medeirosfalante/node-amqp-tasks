var amqp = require('amqplib');


export class NodeAmpqTasks {


  constructor(uri:string,group:string) {
    this.amqp = amqp.connect(`amqp://${uri}`)
    this.group = group;

  }

  Publish(taskKey,body){
    return new Promise((resolve,reject) =>{
      this.amqp.then((conn)  => {
        this.conn = conn;
        return conn.createChannel();
      }).then((ch) => {
        const corr = this.generateUuid()
        return ch.assertQueue('').then((q) => {
          ch.consume(q.queue,(msg) => {
            if (msg.properties.correlationId == corr) {
              resolve(msg.content.toString())
              this.conn.close();
            }
          }, {noAck: true});
          return ch.sendToQueue(`${this.group}-${taskKey}`, new Buffer(JSON.stringify(body)),{ correlationId: corr, replyTo: q.queue });
        });
      }).catch(console.warn);
    })
  }

  generateUuid() {
    return Math.random().toString() +
           Math.random().toString() +
           Math.random().toString();
  }


  On(taskKey:string,cb:Function){
    this.amqp.then((conn) => {
      return conn.createChannel();
    }).then((ch) => {
      return ch.assertQueue(`${this.group}-${taskKey}`).then((ok) => {
        return ch.consume(`${this.group}-${taskKey}`, (msg:string) => cb(ch,`${this.group}-${taskKey}`,msg,msg.content.toString())); 
      }); 
    }).catch(console.warn);
  }

}