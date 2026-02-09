import aws_cdk as core
import aws_cdk.assertions as assertions

from infra.stacks.lakehouse_stack import LakehouseStack

def test_stack_created():
    app = core.App()
    stack = LakehouseStack(app, "aws-lakehouse-template")
    template = assertions.Template.from_stack(stack)
    
    # Just checking it synthesizes for now
    template.resource_count_is("AWS::CDK::Metadata", 0) # Metadata is excluded by assertions by default usually? No, resource count check.
    # Actually, let's just assert the template is created.
    assert template
