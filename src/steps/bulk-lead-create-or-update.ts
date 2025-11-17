/*tslint:disable:no-else-after-return*/

import { BaseStep, Field, StepInterface, ExpectedRecord } from '../core/base-step';
import { Step, FieldDefinition, StepDefinition, RecordDefinition } from '../proto/cog_pb';

export class BulkCreateOrUpdateLeadByFieldStep extends BaseStep implements StepInterface {

  protected stepName: string = 'Bulk create or update Marketo leads';
  protected stepExpression: string = 'bulk create or update marketo leads';
  protected stepType: StepDefinition.Type = StepDefinition.Type.ACTION;
  protected actionList: string[] = ['create', 'update'];
  protected targetObject: string = 'Leads';
  protected expectedFields: Field[] = [
    {
      field: 'partitionId',
      type: FieldDefinition.Type.NUMERIC,
      optionality: FieldDefinition.Optionality.OPTIONAL,
      description: 'ID of partition the lead will be created',
    },
    {
      field: 'leads',
      type: FieldDefinition.Type.MAP,
      description: 'A map of field names to field values',
    },
  ];
  protected expectedRecords: ExpectedRecord[] = [{
    id: 'passedLeads',
    type: RecordDefinition.Type.TABLE,
    fields: [{
      field: 'email',
      type: FieldDefinition.Type.EMAIL,
      description: 'Email of Marketo Lead',
    }, {
      field: 'id',
      type: FieldDefinition.Type.NUMERIC,
      description: 'ID of Marketo Lead',
    }, {
      field: 'message',
      type: FieldDefinition.Type.STRING,
      description: 'Message for explanation of pass',
    }],
  }, {
    id: 'failedLeads',
    type: RecordDefinition.Type.TABLE,
    fields: [{
      field: 'email',
      type: FieldDefinition.Type.EMAIL,
      description: 'Email of Marketo Lead',
    }, {
      field: 'id',
      type: FieldDefinition.Type.NUMERIC,
      description: 'ID of Marketo Lead',
    }, {
      field: 'message',
      type: FieldDefinition.Type.STRING,
      description: 'Message for explanation of fail',
    }],
    dynamicFields: false,
  }];

  async executeStep(step: Step) {
    const stepData: any = step.getData().toJavaScript();
    const partitionId = stepData.partitionId || 1;
    const leads = stepData.leads;
    const leadArray = [];

    Object.values(leads).forEach((lead) => {
      leadArray.push(lead);
    });

    const records = [];
    try {
      const passedLeadArray = [];
      const failedLeadArray = [];

      // we should parse out the original CSV array if provided, or handle it if missing
      const csvArray = stepData.csvArray ? JSON.parse(stepData.csvArray) : [];
      const csvColumns = csvArray[0];
      const csvRows = csvArray.slice(1);
      const failArrayOriginal = csvColumns ? [csvColumns] : [];

      const data: any = await this.client.bulkCreateOrUpdateLead(leadArray, partitionId);

      if (data[0] && data[0].error && !data[0].error.partition) {
        return this.fail('There is no Partition with id %s', [
          partitionId,
        ]);
      }

      // Sort each batch of leads into created, updated, and failed
      data.forEach((batch, i) => {
        const startingIndex = i * 300;
        if (batch.success && batch.result) {
          batch.result.forEach((result, index) => {
            const leadArrayIndex = startingIndex + index;
            if (result.status !== 'skipped') {
              if (['created', 'updated'].includes(result.status)) {
                passedLeadArray.push({ ...leadArray[leadArrayIndex], id: result.id });
              }
            } else if (result.reasons && result.reasons[0]) {
              const errorMessage = result.reasons[0].message;
              // Check if this is the "Multiple lead match lookup criteria" error
              // This should be treated as a pass since one of the leads actually gets updated
              if (errorMessage && errorMessage.toLowerCase().includes('multiple lead match lookup criteria')) {
                // Add to passed leads with a message indicating the special case
                const passedLead: any = { ...leadArray[leadArrayIndex] };
                if (result.id) {
                  passedLead.id = result.id;
                }
                passedLead.message = `Multiple leads matched (one was updated): ${errorMessage}`;
                passedLeadArray.push(passedLead);
              } else {
                // Regular error, add to failed leads
                failedLeadArray.push({ ...leadArray[leadArrayIndex], message: errorMessage });

                // also preserve the original csv entry;
                const match = csvRows[leadArrayIndex];
                if (match) {
                  failArrayOriginal.push(match);
                }
              }

            } else {
              failedLeadArray.push({ ...leadArray[leadArrayIndex], message: result.status });

              const match = csvRows[leadArrayIndex];
              if (match) {
                failArrayOriginal.push(match);
              }
            }
          });
        } else {
          // if the entire batch failed
          const failedLeads = leadArray.slice(startingIndex, startingIndex + 300);
          failedLeads.forEach((lead, index) => {
            failedLeadArray.push({ ...lead, message: 'Marketo request failed' });
            const match = csvRows[startingIndex + index];
            if (match) {
              failArrayOriginal.push(match);
            }
          });
        }
      });

      const returnedLeadsCount = passedLeadArray.length + failedLeadArray.length;

      if (returnedLeadsCount === 0) {
        return this.fail('No leads were created or updated in Marketo', [], []);
      } else if (leadArray.length !== returnedLeadsCount) {
        records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        records.push(this.createTable('failedLeads', 'Leads Failed', failedLeadArray));
        records.push(this.keyValue('failedOriginal', 'Objects Failed (Original format)', { array: JSON.stringify(failArrayOriginal) }));
        return this.fail(
          'Only %d of %d leads were successfully sent to Marketo',
          [returnedLeadsCount, leadArray.length],
          records,
        );
      } else if (!failedLeadArray.length) {
        records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        return this.pass(
          'Successfully created or updated %d leads',
          [passedLeadArray.length],
          records,
        );
      } else {
        records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        records.push(this.createTable('failedLeads', 'Leads Failed', failedLeadArray));
        records.push(this.keyValue('failedOriginal', 'Objects Failed (Original format)', { array: JSON.stringify(failArrayOriginal) }));
        return this.fail(
          'Failed to create or update %d leads',
          [failedLeadArray.length],
          records,
        );
      }
    } catch (e) {
      return this.error('There was an error creating or updating leads in Marketo: %s', [
        e.toString(),
      ]);
    }
  }

  private createTable(id, name, leads) {
    const headers = {};
    const headerKeys = Object.keys(leads[0] || {});
    headerKeys.forEach((key: string) => {
      headers[key] = key;
    });
    return this.table(id, name, headers, leads);
  }
}

export { BulkCreateOrUpdateLeadByFieldStep as Step };
