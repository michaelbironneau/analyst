import { Injectable } from '@angular/core';
import { Observable, Subject} from 'rxjs/Rx';
import * as Rx from 'rxjs/Rx';
import { Message } from './message.model';

const WS_URL = 'ws://localhost:4040';

@Injectable()
export class WebsocketService {
	public messages: Subject<Message>;

  constructor() { 
	  	this.messages = <Subject<Message>>this.connect(WS_URL)
			.map((response: MessageEvent): Message => {
				let data = JSON.parse(response.data);
				return {
					type: data.type,
					data: data.data
				}
			});
  }
  private subject: Rx.Subject<MessageEvent>;

  public connect(url): Rx.Subject<MessageEvent> {
	  console.log("starting WS...");
    if (!this.subject) {
      this.subject = this.create(url);
      console.log("WS connection created: " + url);
    } 
    return this.subject;
  }

  private create(url): Rx.Subject<MessageEvent> {
    let ws = new WebSocket(url);

    let observable = Rx.Observable.create(
	(obs: Rx.Observer<MessageEvent>) => {
		ws.onmessage = obs.next.bind(obs);
		ws.onerror = obs.error.bind(obs);
		ws.onclose = obs.complete.bind(obs);
		return ws.close.bind(ws);
	})
let observer = {
		next: (data: Object) => {
			if (ws.readyState === WebSocket.OPEN) {
				ws.send(JSON.stringify(data));
			}
		}
	}
	return Rx.Subject.create(observer, observable);
  }
}
