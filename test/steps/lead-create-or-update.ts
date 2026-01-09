import { Struct } from 'google-protobuf/google/protobuf/struct_pb';
import * as chai from 'chai';
import { default as sinon } from 'ts-sinon';
import * as sinonChai from 'sinon-chai';
import 'mocha';

import { Step as ProtoStep, StepDefinition, RunStepResponse } from '../../src/proto/cog_pb';
import { Step } from '../../src/steps/lead-create-or-update';

chai.use(sinonChai);

describe('CreateOrUpdateLeadByFieldStep', () => {
  const expect = chai.expect;
  let protoStep: ProtoStep;
  let stepUnderTest: Step;
  let clientWrapperStub: any;

  beforeEach(() => {
    protoStep = new ProtoStep();
    clientWrapperStub = sinon.stub();
    clientWrapperStub.createOrUpdateLead = sinon.stub();
    clientWrapperStub.findLeadByEmail = sinon.stub();
    stepUnderTest = new Step(clientWrapperStub);
  });

  it('should return expected step metadata', () => {
    const stepDef: StepDefinition = stepUnderTest.getDefinition();
    expect(stepDef.getStepId()).to.equal('CreateOrUpdateLeadByFieldStep');
    expect(stepDef.getName()).to.equal('Create or update a Marketo lead');
    expect(stepDef.getExpression()).to.equal('create or update a marketo lead');
    expect(stepDef.getType()).to.equal(StepDefinition.Type.ACTION);
  });

  it('should call the client wrapper with the expected args', async () => {
    protoStep.setData(Struct.fromJavaScript({
      email: 'sampleEmail@example.com',
    }));

    await stepUnderTest.executeStep(protoStep);
    expect(clientWrapperStub.createOrUpdateLead).to.have.been.calledWith(
      protoStep.getData().toJavaScript().lead,
    );
  });

  it('should respond with success if the marketo executes succesfully', async () => {
    const expectedEmail: string = 'expected@example.com';
    const expectedReason: string = 'reason it failed';
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'success',
          id: 123321,
        },
      ],
    }));
    clientWrapperStub.findLeadByEmail.returns(Promise.resolve({
      success: true,
      result: [
        {
          id: 123321,
          email: 'any@test.com',
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      lead: {
        email: 'sampleEmail@example.com',
      },
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
  });

  it('should respond with fail if the partition does not exist', async () => {
    const expectedEmail: string = 'expected@example.com';
    const expectedReason: string = 'reason it failed';
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({ error: { partition: false } }));
    protoStep.setData(Struct.fromJavaScript({
      lead: {
        email: 'sampleEmail@example.com',
      },
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should respond with fail if the marketo skips creation of lead with reason', async () => {
    const expectedReason: string = 'reason it failed';
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
          reasons: [
            {
              message: expectedReason,
            },
          ],
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      email: 'sampleEmail@example.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageArgsList()[0].getStringValue()).to.equal(expectedReason);
  });

  it('should respond with fail if the marketo skips creation of lead', async () => {
    const expectedMessage: string  = 'status was skipped';
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      email: 'sampleEmail@example.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageArgsList()[0].getStringValue()).to.equal(expectedMessage);
  });

  it('should respond with an error if the marketo throws an error', async () => {
    clientWrapperStub.createOrUpdateLead.throws('any error');
    protoStep.setData(Struct.fromJavaScript({
      email: 'any@email.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
  });

  it('should respond with fail if multiple leads match and updateMostRecentMatch is false', async () => {
    const duplicateErrorMessage: string = 'Multiple lead match lookup criteria';
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
          reasons: [
            {
              message: duplicateErrorMessage,
            },
          ],
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      lead: {
        email: 'duplicate@example.com',
      },
      updateMostRecentMatch: false,
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageArgsList()[0].getStringValue()).to.equal(duplicateErrorMessage);
  });

  it('should update most recent lead when multiple leads match and updateMostRecentMatch is true', async () => {
    const duplicateErrorMessage: string = 'Multiple lead match lookup criteria';
    const email: string = 'duplicate@example.com';
    
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
          reasons: [
            {
              message: duplicateErrorMessage,
            },
          ],
        },
      ],
    }));
    
    // Mock findLeadByEmail to return multiple leads
    clientWrapperStub.findLeadByEmail.onFirstCall().returns(Promise.resolve({
      success: true,
      result: [
        {
          id: 123321,
          email,
          updatedAt: '2023-01-01T00:00:00Z',
          createdAt: '2023-01-01T00:00:00Z',
        },
        {
          id: 123322,
          email,
          updatedAt: '2023-06-01T00:00:00Z',
          createdAt: '2023-01-01T00:00:00Z',
        },
        {
          id: 123323,
          email,
          updatedAt: '2023-03-01T00:00:00Z',
          createdAt: '2023-01-01T00:00:00Z',
        },
      ],
    }));
    
    clientWrapperStub.updateLead = sinon.stub();
    clientWrapperStub.updateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'updated',
          id: 123322,
        },
      ],
    }));
    
    // Mock the second findLeadByEmail call (after update)
    clientWrapperStub.findLeadByEmail.onSecondCall().returns(Promise.resolve({
      success: true,
      result: [
        {
          id: 123322,
          email,
          updatedAt: '2023-06-01T00:00:00Z',
          firstName: 'Updated',
        },
      ],
    }));
    
    protoStep.setData(Struct.fromJavaScript({
      lead: {
        email,
        firstName: 'Updated',
      },
      updateMostRecentMatch: true,
    }));
    
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.PASSED);
    expect(clientWrapperStub.updateLead).to.have.been.calledOnce;
    // Verify it updated the most recent lead (ID 123322 with updatedAt 2023-06-01)
    expect(clientWrapperStub.updateLead.firstCall.args[1]).to.equal('id');
    expect(clientWrapperStub.updateLead.firstCall.args[2]).to.equal('123322');
  });

  it('should fail if updateMostRecentMatch is true but update fails', async () => {
    const duplicateErrorMessage: string = 'Multiple lead match lookup criteria';
    const email: string = 'duplicate@example.com';
    
    clientWrapperStub.createOrUpdateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
          reasons: [
            {
              message: duplicateErrorMessage,
            },
          ],
        },
      ],
    }));
    
    clientWrapperStub.findLeadByEmail.returns(Promise.resolve({
      success: true,
      result: [
        {
          id: 123321,
          email,
          updatedAt: '2023-01-01T00:00:00Z',
        },
      ],
    }));
    
    clientWrapperStub.updateLead = sinon.stub();
    clientWrapperStub.updateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
          reasons: [
            {
              message: 'Update failed',
            },
          ],
        },
      ],
    }));
    
    protoStep.setData(Struct.fromJavaScript({
      lead: {
        email,
        firstName: 'Updated',
      },
      updateMostRecentMatch: true,
    }));
    
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

});
