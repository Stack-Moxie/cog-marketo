/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition, StepRecord } from '../proto/cog_pb';

export class UpdateLeadStep extends BaseStep implements StepInterface {

  protected stepName: string = 'Update a Marketo lead';
  protected stepExpression: string = 'update an existing marketo lead';
  protected stepType: StepDefinition.Type = StepDefinition.Type.ACTION;
  protected actionList: string[] = ['update'];
  protected targetObject: string = 'Lead';
  protected expectedFields: Field[] = [
    {
      field: 'partitionId',
      type: FieldDefinition.Type.NUMERIC,
      optionality: FieldDefinition.Optionality.OPTIONAL,
      description: 'ID of partition the lead will be created',
    },
    {
      field: 'reference',
      type: FieldDefinition.Type.STRING,
      description: 'Lead\'s email address or id',
    },
    {
      field: 'lead',
      type: FieldDefinition.Type.MAP,
      description: 'A map of field names to field values',
    },
  ];
  protected expectedRecords: ExpectedRecord[] = [{
    id: 'lead',
    type: RecordDefinition.Type.KEYVALUE,
    fields: [{
      field: 'id',
      type: FieldDefinition.Type.NUMERIC,
      description: "Lead's Marketo ID",
    }, {
      field: 'email',
      type: FieldDefinition.Type.NUMERIC,
      description: "Lead's Email",
    }],
    dynamicFields: true,
  }];

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const partitionId = stepData.partitionId || 1;
    const reference = stepData.reference;
    const lead = stepData.lead;

    try {
      const emailRegex = /(.+)@(.+){2,}\.(.+){2,}/;
      let lookupField = 'id';
      if (emailRegex.test(reference)) {
        lookupField = 'email';
      } else {
        lead['id'] = reference;
      }

      const data: any = await this.client.updateLead(lead, lookupField, reference, partitionId);
      if (data.success && data.result && data.result[0] && data.result[0].status !== 'skipped') {
        const updatedLead: any = await this.client.findLeadByField(lookupField, reference, null, partitionId);
        const record = this.createRecord(updatedLead.result[0]);
        const orderedRecord = this.createOrderedRecord(updatedLead.result[0], stepData['__stepOrder']);
        return this.pass(
          'Successfully updated lead %s',
          [reference],
          [record, orderedRecord],
        );
      } else if (data && data.error && !data.error.partition) {
        return this.fail('There is no Partition with id %s', [
          partitionId,
        ]);
      } else {
        if (data.result && data.result[0] && data.result[0].reasons && data.result[0].reasons[0]) {
          return this.fail('Unable to update lead: %s', [
            data.result[0].reasons[0].message,
          ]);
        } else {
          return this.fail('Unable to update lead: %s', [
            `status was ${data.result[0].status}`,
          ]);
        }
      }
    } catch (e) {
      return this.error('There was an error updating leads in Marketo: %s', [
        e.toString(),
      ]);
    }
  }

  public createRecord(lead): StepRecord {
    return this.keyValue('lead', 'Updated Lead', lead);
  }

  public createOrderedRecord(lead, stepOrder = 1): StepRecord {
    return this.keyValue(`lead.${stepOrder}`, `Updated Lead from Step ${stepOrder}`, lead);
  }

}

export { UpdateLeadStep as Step };
