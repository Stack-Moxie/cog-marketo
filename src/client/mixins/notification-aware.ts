import * as mailgun from 'mailgun-js';

export interface NotificationMessage {
  subject: string;
  from: string;
  receivedAt: string;
  body: string;
}

export class NotificationAwareMixin {
  client: any;
  notificationInbox: string;

  public getNotificationInbox(): string {
    return this.notificationInbox || '';
  }

  public async getNotificationMessages(inbox: string): Promise<NotificationMessage[]> {
    const creds = (this.client && this.client.mailgunCredentials) || {};
    if (!creds.apiKey || !creds.domain) {
      throw new Error('Mailgun is not configured on the Marketo cog');
    }
    if (!inbox) {
      throw new Error('Notification inbox address is missing');
    }

    const mg = mailgun({ apiKey: creds.apiKey, domain: creds.domain });
    const events: any = await new Promise((resolve, reject) => {
      mg.get(`/${creds.domain}/events`, { recipient: inbox, limit: 25 }, (err, body) => {
        if (err) {
          return reject(err);
        }
        resolve(body);
      });
    });

    const items = (events && events.items) || [];
    return items.map((ev: any) => {
      const headers = (ev.message && ev.message.headers) || {};
      return {
        subject: headers.subject || '',
        from: headers.from || '',
        receivedAt: ev.timestamp ? new Date(ev.timestamp * 1000).toISOString() : '',
        body: '',
      };
    });
  }
}
