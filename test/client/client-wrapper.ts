import * as chai from 'chai';
import { default as sinon } from 'ts-sinon';
import * as sinonChai from 'sinon-chai';
import 'mocha';

import { ClientWrapper } from '../../src/client/client-wrapper';
import { Metadata } from 'grpc';

chai.use(sinonChai);

describe('ClientWrapper', () => {
  const expect = chai.expect;
  let marketoClientStub: any;
  let marketoConstructorStub: any;
  let metadata: Metadata;
  let clientWrapperUnderTest: ClientWrapper;

  beforeEach(() => {
    marketoClientStub = {
      lead: {
        find: sinon.stub(),
        createOrUpdate: sinon.stub(),
        describe: sinon.stub(),
        partitions: sinon.stub(),
      },
      activities: {
        getActivityTypes: sinon.stub(),
      },
      _connection: {
        postJson: sinon.stub(),
        get: sinon.stub(),
        post: sinon.stub(),
      },
    };
    marketoConstructorStub = sinon.stub();
    marketoClientStub.campaign = sinon.stub();
    marketoClientStub.campaign.getCampaigns = sinon.stub();
    marketoClientStub.campaign.request = sinon.stub();
    marketoClientStub.lead.describe = sinon.stub();
    marketoClientStub.lead.describe.returns(Promise.resolve({
      result: [{
        rest: {
          name: 'email',
        },
      }],
    }));
    marketoClientStub.lead.partitions = sinon.stub();
    marketoClientStub.lead.partitions.resolves({ result: [
      { id: 1, name: 'Default' },
    ]});
    marketoConstructorStub.returns(marketoClientStub);
    marketoClientStub.lead.mergeLead = sinon.stub();
  });

  it('authentication', () => {
    // Construct grpc metadata and assert the client was authenticated.
    const expectedCallArgs = {
      endpoint: 'https://abc-123-xyz.mktorest.example/rest',
      identity: 'https://abc-123-xyz.mktorest.example/identity',
      clientId: 'a-client-id',
      clientSecret: 'a-client-secret',
    };
    metadata = new Metadata();
    metadata.add('endpoint', expectedCallArgs.endpoint.replace('/rest', ''));
    metadata.add('clientId', expectedCallArgs.clientId);
    metadata.add('clientSecret', expectedCallArgs.clientSecret);

    // Assert that the underlying API client was authenticated correctly.
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    expect(marketoConstructorStub).to.have.been.calledWith(expectedCallArgs);
  });

  it('authentication::partner', () => {
    // Construct grpc metadata and assert the client was authenticated.
    const expectedCallArgs = {
      endpoint: 'https://abc-123-xyz.mktorest.example/rest',
      identity: 'https://abc-123-xyz.mktorest.example/identity',
      clientId: 'a-client-id',
      clientSecret: 'a-client-secret',
      partnerId: 'some-partner-id',
    };
    metadata = new Metadata();
    metadata.add('endpoint', expectedCallArgs.endpoint.replace('/rest', ''));
    metadata.add('clientId', expectedCallArgs.clientId);
    metadata.add('clientSecret', expectedCallArgs.clientSecret);
    metadata.add('partnerId', expectedCallArgs.partnerId);

    // Assert that the underlying API client was authenticated correctly.
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    expect(marketoConstructorStub).to.have.been.calledWith(expectedCallArgs);
  });

  it('createOrUpdateLead', (done) => {
    const expectedLead = { email: 'test@example.com',  };
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.createOrUpdateLead(expectedLead);

    setTimeout(() => {
      expect(marketoClientStub.lead.createOrUpdate).to.have.been.calledWith(
        [expectedLead],
        { lookupField: 'email', partitionName: 'Default'},
      );
      done();
    });
  });

  it('bulkCreateOrUpdateLead', (done) => {
    const expectedLeads = [
      { email: 'test1@example.com' },
      { email: 'test2@example.com' },
      { email: 'test3@example.com' },
    ];
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.bulkCreateOrUpdateLead(expectedLeads);

    setTimeout(() => {
      expect(marketoClientStub.lead.createOrUpdate).to.have.been.calledWith(
        expectedLeads,
        { lookupField: 'email', partitionName: 'Default' },
      );
      done();
    });
  });

  it('findLeadByEmail (no options)', (done) => {
    const expectedEmail = 'test@example.com';
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.findLeadByEmail(expectedEmail);

    setTimeout(() => {
      expect(marketoClientStub.lead.find).to.have.been.calledWith(
        'email',
        [expectedEmail],
        { fields: ['email'] },
      );
      done();
    });
  });

  it('findLeadByEmail (with options)', (done) => {
    const expectedEmail = 'test@example.com';
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.findLeadByEmail(expectedEmail);

    setTimeout(() => {
      expect(marketoClientStub.lead.find).to.have.been.calledWith(
        'email',
        [expectedEmail],
        { fields: ['email'] },
      );

      done();
    });
  });

  it('findLeadByEmail (with partition id)', (done) => {
    const expectedEmail = 'test-in-partition-2@example.com';
    const expectedPartition = 1;

    marketoClientStub.lead.find = sinon.stub();
    marketoClientStub.lead.find.returns(Promise.resolve({
      result: [{
        email: `not-${expectedEmail}`,
        leadPartitionId: expectedPartition * 2,
      }, {
        email: expectedEmail,
        leadPartitionId: expectedPartition,
      }],
    }));

    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.findLeadByEmail(expectedEmail, null, expectedPartition).then((res) => {
      try {
        expect(res.result[0].email).to.equal(expectedEmail);
      } catch (e) {
        return done(e);
      }
      done();
    });
  });

  it('deleteLeadById', () => {
    const expectedId = 123;
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.deleteLeadById(expectedId);

    expect(marketoClientStub._connection.postJson).to.have.been.calledWith(
      '/v1/leads.json',
      { input: [ { id: expectedId } ] },
      { query: { _method: 'DELETE' } }
    );
  });

  it('describeLeadFields', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.describeLeadFields();

    expect(marketoClientStub.lead.describe).to.have.been.called;
  });

  it('addLeadToSmartCampaign', () => {
    const campaignIdInput = 'someId';
    const leadInput = { name: 'someLead' };
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.addLeadToSmartCampaign(campaignIdInput, leadInput);

    expect(marketoClientStub.campaign.request).to.have.been.calledWith(campaignIdInput, [leadInput]);
  });

  // it('getCampaigns', () => {
  //   clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
  //   clientWrapperUnderTest.getCampaigns();

  //   expect(marketoClientStub.campaign.getCampaigns).to.have.been.calledWith();
  // });

  it('mergeLeadsById', (done) => {
    const winningId = '1';
    const losingIds = ['2'];
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.mergeLeadsById(winningId, losingIds);

    setTimeout(() => {
      expect(marketoClientStub.lead.mergeLead).to.have.been.calledWith(
        winningId,
        losingIds,
        { mergeInCrm: null },
      );
      done();
    });
  });

  it('createOrUpdateCustomObject', () => {
    const customObjectName = 'any';
    const customObject = { anyField: 'anyValue'};
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.createOrUpdateCustomObject(customObjectName, customObject);

    expect(marketoClientStub._connection.postJson).to.have.been.calledWith(
      `/v1/customobjects/${customObjectName}.json`,
      {
        action: 'createOrUpdate',
        dedupeBy: 'dedupeFields',
        input: [customObject],
      },
      {
        query: {
          _method: 'POST',
        },
      },
    );
  });

  it('getCustomObject', () => {
    const customObjectName = 'any';
    const customObject = { anyField: 'anyValue' };
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getCustomObject(customObjectName);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(
      `/v1/customobjects/${customObjectName}/describe.json`,
      { query: { _method: 'GET' } },
    );
  });

  it('queryCustomObject(mutiple searchFields)', async () => {
    const customObjectName = 'any';
    const filterType = 'anyFilterType';
    const searchFields = [{ anySearchField: 'anySearchFieldValue' }];
    const requestFields = ['anyField'];
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.customObjectDescriptions = {
      [customObjectName]: {result: [{fields: requestFields.map(f => {return {name: f}})}]},
    },
    await clientWrapperUnderTest.queryCustomObject(customObjectName, filterType, searchFields);

    expect(marketoClientStub._connection.postJson).to.have.been.calledWith(
      `/v1/customobjects/${customObjectName}.json`,
      {
        filterType: `${filterType}`,
        fields: requestFields,
        input: searchFields,
      },
      {
        query: {
          _method: 'GET' ,
        },
      },
    );
  });

  it('queryCustomObject(single searchFields)', async () => {
    const customObjectName = 'any';
    const filterType = 'anyFilterType';
    const searchFields = ['anySearchFieldValue'];
    const requestFields = ['anyField'];
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.customObjectDescriptions = {
      [customObjectName]: {result: [{fields: requestFields.map(f => {return {name: f}})}]},
    },
    await clientWrapperUnderTest.queryCustomObject(customObjectName, filterType, searchFields);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(
      `/v1/customobjects/${customObjectName}.json?filterType=${filterType}&filterValues=${searchFields.join(',')}&fields=${requestFields.join(',')}`,
    );
  });

  it('deleteCustomObjectById', () => {
    const customObjectName = 'any';
    const customObjectGUID = 'anyGUID';
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.deleteCustomObjectById(customObjectName, customObjectGUID);

    expect(marketoClientStub._connection.postJson).to.have.been.calledWith(
      `/v1/customobjects/${customObjectName}/delete.json`,
      {
        deleteBy: 'idField',
        input: [{
          marketoGUID: customObjectGUID,
        }],
      },
    );
  });

  it('getActivityTypes', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getActivityTypes();

    expect(marketoClientStub.activities.getActivityTypes).to.have.been.calledWith();
  });

  it('getActivityPagingToken', () => {
    const sinceDate = 'anyDate';
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getActivityPagingToken(sinceDate);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(
      `/v1/activities/pagingtoken.json?sinceDatetime=${sinceDate}`,
    );
  });

  it('getActivitiesByLeadId', () => {
    const nextPageToken = 'anyToken';
    const leadId = 'anyId';
    const activityId = 'anyActivityId';
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getActivitiesByLeadId(nextPageToken, leadId, activityId);

    expect(marketoClientStub._connection.get).to.have.been.calledWith('/v1/activities.json', {
      query: {
        nextPageToken,
        leadIds: leadId,
        activityTypeIds: activityId,
      },
    });
  });

  it('getDailyApiUsage', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getDailyApiUsage();

    expect(marketoClientStub._connection.get).to.have.been.calledWith('/v1/stats/usage.json');
  });

  it('getWeeklyApiUsage', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.getWeeklyApiUsage();

    expect(marketoClientStub._connection.get).to.have.been.calledWith('/v1/stats/usage/last7days.json');
  });

  it('getBulkExportLeadJobs uses bulk URL and query _method', async () => {
    metadata = new Metadata();
    metadata.add('endpoint', 'https://123-abc-456.mktorest.com');
    metadata.add('clientId', 'a-client-id');
    metadata.add('clientSecret', 'a-client-secret');
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    marketoClientStub._connection._options = {
      endpoint: 'https://123-abc-456.mktorest.com/rest',
    };
    marketoClientStub._connection.get.returns(Promise.resolve({ success: true, result: [] }));

    await clientWrapperUnderTest.getBulkExportLeadJobs();

    expect(marketoClientStub._connection.get).to.have.been.calledWith(
      'https://123-abc-456.mktorest.com/bulk/v1/leads/export.json',
      { query: { _method: 'GET' } },
    );
  });

  it('bulkUrl strips a trailing slash after /rest', () => {
    metadata = new Metadata();
    metadata.add('endpoint', 'https://123-abc-456.mktorest.com');
    metadata.add('clientId', 'a-client-id');
    metadata.add('clientSecret', 'a-client-secret');
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    marketoClientStub._connection._options = {
      endpoint: 'https://123-abc-456.mktorest.com/rest/',
    };

    expect(clientWrapperUnderTest.bulkUrl('activities/export.json')).to.equal(
      'https://123-abc-456.mktorest.com/bulk/v1/activities/export.json',
    );
  });

  it('getFoldersById', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedId = '123';
    clientWrapperUnderTest.getFoldersById(expectedId);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/folder/${expectedId}.json?type=Folder`);
  });

  it('createProgram', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const sampleProgram = { name: '123' };
    clientWrapperUnderTest.createProgram(sampleProgram);

    expect(marketoClientStub._connection.post).to.have.been.calledWith('/asset/v1/programs.json?' + sampleProgram);
  });

  it('updateProgram', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const sampleId = '123';
    const sampleProgram = 'sampleString';
    clientWrapperUnderTest.updateProgram(sampleId, sampleProgram);

    expect(marketoClientStub._connection.post).to.have.been.calledWith(`/asset/v1/program/${sampleId}.json?` + sampleProgram);
  });

  it('getPrograms', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedId = '123';
    clientWrapperUnderTest.getPrograms();

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/programs.json`);
  });

  it('findProgramsByName', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const sampleName = 'asd';
    clientWrapperUnderTest.findProgramsByName(sampleName);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/program/byName.json?name=${sampleName}&includeCosts=true`);
  });

  it('findProgramsById', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const sampleId = 'asd';
    clientWrapperUnderTest.findProgramsById(sampleId);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/program/${sampleId}.json`);
  });

  it('deleteProgramById', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedId = '123';
    clientWrapperUnderTest.deleteProgramById(expectedId);

    expect(marketoClientStub._connection.post).to.have.been.calledWith(`/asset/v1/program/${expectedId}/delete.json`);
  });

  // Static List Aware
  it('findStaticListsByName', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedName = 'anyName';
    clientWrapperUnderTest.findStaticListsByName(expectedName);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/staticList/byName.json?name=${expectedName}`);
  });
  
  it('findStaticLists', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    clientWrapperUnderTest.findStaticLists();

    expect(marketoClientStub._connection.get).to.have.been.calledWith('/asset/v1/staticLists.json');
  });

  it('findStaticListsById', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedId = '123';
    clientWrapperUnderTest.findStaticListsById(expectedId);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/asset/v1/staticList/${expectedId}.json`);
  });

  it('findStaticListsMembershipByListId', () => {
    clientWrapperUnderTest = new ClientWrapper(metadata, marketoConstructorStub, 0);
    const expectedId = '123';
    clientWrapperUnderTest.findStaticListsMembershipByListId(expectedId);

    expect(marketoClientStub._connection.get).to.have.been.calledWith(`/v1/lists/${expectedId}/leads.json?batchSize=300`);
  });
});
