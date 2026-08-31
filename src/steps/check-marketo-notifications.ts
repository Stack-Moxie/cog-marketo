/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition } from '../proto/cog_pb';

export class CheckMarketoNotificationsStep extends BaseStep implements StepInterface {

  protected stepName: string = 'Check Marketo notifications';
  protected stepExpression: string = 'there should be no marketo notification matching (?<regex>.+)';
  protected stepType: StepDefinition.Type = StepDefinition.Type.VALIDATION;
  protected actionList: string[] = ['check'];
  protected targetObject: string = 'Marketo Notifications';
  protected stepHelp: string = 'Matches recent CRM Sync notification emails for this connection\'s subscribe address. Fail if any message matches the regex. Schedule the scenario like any other check (Frequency on the scenario).';
  protected expectedFields: Field[] = [{
    field: 'regex',
    type: FieldDefinition.Type.STRING,
    description: 'Regex to match against notification subject and from',
  }];
  protected expectedRecords: ExpectedRecord[] = [{
    id: 'messages',
    type: RecordDefinition.Type.TABLE,
    fields: [{
      field: 'subject',
      type: FieldDefinition.Type.STRING,
      description: 'Email subject line',
    }, {
      field: 'from',
      type: FieldDefinition.Type.STRING,
      description: 'Email from line',
    }, {
      field: 'receivedAt',
      type: FieldDefinition.Type.STRING,
      description: 'When Mailgun received the message',
    }],
    dynamicFields: false,
  }];

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const pattern = (stepData.regex || '').trim();
    const inbox = this.client.getNotificationInbox ? this.client.getNotificationInbox() : '';

    if (!pattern) {
      return this.error('A regex is required');
    }

    let matcher: RegExp;
    try {
      matcher = new RegExp(pattern, 'i');
    } catch (e) {
      return this.error('Invalid regex: %s', [e.toString()]);
    }

    if (!inbox) {
      return this.error('Notification inbox is missing on this Marketo connection. Copy the subscribe address from the connection and add it in Marketo Notifications.');
    }

    try {
      const messages = await this.client.getNotificationMessages(inbox);
      const matching = (messages || []).filter((msg: any) => {
        const haystack = [msg.subject, msg.from, msg.body].filter(Boolean).join('\n');
        return matcher.test(haystack);
      });

      const table = this.table('messages', 'Marketo Notifications', {
        subject: 'Subject',
        from: 'From',
        receivedAt: 'Received',
      }, matching.map((msg: any) => ({
        subject: msg.subject,
        from: msg.from,
        receivedAt: msg.receivedAt,
      })));

      if (matching.length > 0) {
        return this.fail(
          '%d Marketo notification(s) matched /%s/ for %s',
          [matching.length, pattern, inbox],
          [table],
        );
      }

      return this.pass(
        'No Marketo notifications matched /%s/ for %s (%d recent message(s) checked)',
        [pattern, inbox, (messages || []).length],
        [table],
      );
    } catch (e) {
      return this.error('There was a problem checking Marketo notifications: %s', [e.toString()]);
    }
  }
}

export { CheckMarketoNotificationsStep as Step };
