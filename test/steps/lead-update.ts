import { Struct } from 'google-protobuf/google/protobuf/struct_pb';
import * as chai from 'chai';
import { default as sinon } from 'ts-sinon';
import * as sinonChai from 'sinon-chai';
import 'mocha';

import { Step as ProtoStep, StepDefinition, RunStepResponse } from '../../src/proto/cog_pb';
import { Step } from '../../src/steps/lead-update';

chai.use(sinonChai);

describe('UpdateLeadStep', () => {
  const expect = chai.expect;
  let protoStep: ProtoStep;
  let stepUnderTest: Step;
  let clientWrapperStub: any;

  beforeEach(() => {
    protoStep = new ProtoStep();
    clientWrapperStub = sinon.stub();
    clientWrapperStub.updateLead = sinon.stub();
    clientWrapperStub.findLeadByEmail = sinon.stub();
    clientWrapperStub.findLeadByField = sinon.stub();
    stepUnderTest = new Step(clientWrapperStub);
  });

  it('should return expected step metadata', () => {
    const stepDef: StepDefinition = stepUnderTest.getDefinition();
    expect(stepDef.getStepId()).to.equal('UpdateLeadStep');
    expect(stepDef.getName()).to.equal('Update a Marketo lead');
    expect(stepDef.getExpression()).to.equal('update an existing marketo lead');
    expect(stepDef.getType()).to.equal(StepDefinition.Type.ACTION);
  });

  it('should call the client wrapper with the expected args', async () => {
    protoStep.setData(Struct.fromJavaScript({
      reference: 'sampleEmail@example.com',
    }));

    await stepUnderTest.executeStep(protoStep);
    expect(clientWrapperStub.updateLead).to.have.been.calledWith(
      protoStep.getData().toJavaScript().lead,
    );
  });

  it('should respond with success if the marketo executes succesfully', async () => {
    const expectedEmail: string = 'expected@example.com';
    const expectedReason: string = 'reason it failed';
    clientWrapperStub.updateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'success',
          id: 123321,
        },
      ],
    }));
    clientWrapperStub.findLeadByField.returns(Promise.resolve({
      success: true,
      result: [
        {
          id: 123321,
          email: 'any@test.com',
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      reference: 123321,
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
    clientWrapperStub.updateLead.returns(Promise.resolve({ error: { partition: false } }));
    protoStep.setData(Struct.fromJavaScript({
      reference: 'anyReference',
      lead: {
        email: 'sampleEmail@example.com',
      },
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
  });

  it('should respond with fail if the marketo skips creation of lead with reason', async () => {
    const expectedReason: string = 'reason it failed';
    clientWrapperStub.updateLead.returns(Promise.resolve({
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
      reference: 'sampleEmail@example.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageArgsList()[0].getStringValue()).to.equal(expectedReason);
  });

  it('should respond with fail if the marketo skips creation of lead', async () => {
    const expectedMessage: string  = 'status was skipped';
    clientWrapperStub.updateLead.returns(Promise.resolve({
      success: true,
      result: [
        {
          status: 'skipped',
        },
      ],
    }));
    protoStep.setData(Struct.fromJavaScript({
      reference: 'sampleEmail@example.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.FAILED);
    expect(response.getMessageArgsList()[0].getStringValue()).to.equal(expectedMessage);
  });

  it('should respond with an error if the marketo throws an error', async () => {
    clientWrapperStub.updateLead.throws('any error');
    protoStep.setData(Struct.fromJavaScript({
      email: 'any@email.com',
    }));
    const response: RunStepResponse = await stepUnderTest.executeStep(protoStep);
    expect(response.getOutcome()).to.equal(RunStepResponse.Outcome.ERROR);
  });

});
