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
    id: 'duplicateLeads',
    type: RecordDefinition.Type.TABLE,
    fields: [{
      field: 'email',
      type: FieldDefinition.Type.EMAIL,
      description: 'Email of Marketo Lead',
    }, {
      field: 'message',
      type: FieldDefinition.Type.STRING,
      description: 'Multiple lead match lookup criteria message',
    }],
    dynamicFields: false,
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
    const updateMostRecentMatch: boolean = stepData.updateMostRecentMatch || false;
    const leadArray = [];

    Object.values(leads).forEach((lead) => {
      leadArray.push(lead);
    });

    const records = [];
    try {
      const passedLeadArray = [];
      const duplicateLeadArray = [];
      const failedLeadArray = [];
      const leadsToResolve = []; // Store leads that need resolution

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
              if (errorMessage && errorMessage.toLowerCase().includes('multiple lead match lookup criteria')) {
                if (updateMostRecentMatch) {
                  // Store for resolution later
                  leadsToResolve.push({ lead: leadArray[leadArrayIndex], index: leadArrayIndex });
                } else {
                  // Add to duplicate leads with the error message (current behavior)
                  const duplicateLead: any = { ...leadArray[leadArrayIndex] };
                  if (result.id) {
                    duplicateLead.id = result.id;
                  }
                  duplicateLead.message = errorMessage;
                  duplicateLeadArray.push(duplicateLead);
                }
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

      // Resolve leads with multiple matches if updateMostRecentMatch is true
      if (updateMostRecentMatch && leadsToResolve.length > 0) {
        for (const { lead, index } of leadsToResolve) {
          try {
            // Find all leads matching the email
            const matchingLeads: any = await this.client.findLeadByEmail(lead.email, null, partitionId);

            if (matchingLeads.success && matchingLeads.result && matchingLeads.result.length > 0) {
              // Sort by updatedAt descending to get the most recent lead
              const sortedLeads = matchingLeads.result.sort((a, b) => {
                const dateA = new Date(a.updatedAt || a.createdAt || 0);
                const dateB = new Date(b.updatedAt || b.createdAt || 0);
                return dateB.getTime() - dateA.getTime();
              });

              const mostRecentLead = sortedLeads[0];

              // Update the most recent lead by ID
              // Create a new lead object with the ID to ensure proper update
              const leadWithId = { ...lead, id: mostRecentLead.id };
              const updateData: any = await this.client.updateLead(leadWithId, 'id', mostRecentLead.id.toString(), partitionId);

              if (updateData.success && updateData.result && updateData.result[0] && updateData.result[0].status !== 'skipped') {
                // Add to passed leads with the updated lead ID
                passedLeadArray.push({ ...lead, id: mostRecentLead.id, message: `Updated most recent of ${matchingLeads.result.length} matching leads` });
              } else {
                // If update failed, add to failed leads
                failedLeadArray.push({
                  ...lead,
                  message: updateData.result && updateData.result[0] && updateData.result[0].reasons
                    ? updateData.result[0].reasons[0].message
                    : 'Failed to update most recent matching lead',
                });
                const match = csvRows[index];
                if (match) {
                  failArrayOriginal.push(match);
                }
              }
            } else {
              // If no matching leads found, add to failed leads
              failedLeadArray.push({ ...lead, message: 'Unable to find matching leads' });
              const match = csvRows[index];
              if (match) {
                failArrayOriginal.push(match);
              }
            }
          } catch (resolveError) {
            // If resolution fails, add to failed leads
            failedLeadArray.push({ ...lead, message: `Error resolving multiple matches: ${resolveError.message}` });
            const match = csvRows[index];
            if (match) {
              failArrayOriginal.push(match);
            }
          }
        }
      }

      const successfulLeadsCount = passedLeadArray.length + duplicateLeadArray.length;
      const returnedLeadsCount = successfulLeadsCount + failedLeadArray.length;

      if (returnedLeadsCount === 0) {
        return this.fail('No leads were created or updated in Marketo', [], []);
      } else if (leadArray.length !== returnedLeadsCount) {
        if (passedLeadArray.length > 0) {
          records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        }
        if (duplicateLeadArray.length > 0) {
          records.push(this.createTable('duplicateLeads', 'Duplicate Leads (one was updated)', duplicateLeadArray));
        }
        if (failedLeadArray.length > 0) {
          records.push(this.createTable('failedLeads', 'Leads Failed', failedLeadArray));
        }
        records.push(this.keyValue('failedOriginal', 'Objects Failed (Original format)', { array: JSON.stringify(failArrayOriginal) }));
        return this.fail(
          'Only %d of %d leads were successfully sent to Marketo',
          [returnedLeadsCount, leadArray.length],
          records,
        );
      } else if (!failedLeadArray.length) {
        if (passedLeadArray.length > 0) {
          records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        }
        if (duplicateLeadArray.length > 0) {
          records.push(this.createTable('duplicateLeads', 'Duplicate Leads (one was updated)', duplicateLeadArray));
        }
        return this.pass(
          'Successfully created or updated %d leads',
          [successfulLeadsCount],
          records,
        );
      } else {
        if (passedLeadArray.length > 0) {
          records.push(this.createTable('passedLeads', 'Leads Created or Updated', passedLeadArray));
        }
        if (duplicateLeadArray.length > 0) {
          records.push(this.createTable('duplicateLeads', 'Duplicate Leads (one was updated)', duplicateLeadArray));
        }
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
