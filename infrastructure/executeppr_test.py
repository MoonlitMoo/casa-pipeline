import pytest

from io import StringIO
from .filenamer import sanitize_for_ms
from .executeppr import _getFirstRequest

test_params = [('uid__A002_target.msXd3e89f_Xc53e', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms_target.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms_target.ms.ms.ms.ms', 'uid__A002_target.msXd3e89f_Xc53e')]


@pytest.mark.parametrize("visname, expected", test_params)
def test_sanitize_ms(visname, expected):
    """Test sanitize_for_ms() from executeppr."""
    assert sanitize_for_ms(visname) == expected


test_parameter_values = [(r"1,2,3", '1,2,3'),
                         (r"(1,2,3)", (1, 2, 3)),
                         (r"[1,2,3]", [1, 2, 3]),
                         (r"true", True),
                         (r"False", False),
                         (r"['','table1.cal']", ['', 'table1.cal']),
                         (r"['test1.ms','test2.ms']", ['test1.ms', 'test2.ms']),
                         (r"test1.ms,test2.ms", 'test1.ms,test2.ms'),
                         (r"&lt;12km", '<12km'),
                         (r"None", 'None'),
                         (r"[[0,0,1,1],[0,1,0,1]]", [[0, 0, 1, 1], [0, 1, 0, 1]])]


def create_example_alma_ppr(input_parameter_str):
    """Create an example ppr to test task paramater value parsing."""
    template_ppr = """\
<?xml version="1.0" encoding="UTF-8"?>
<SciPipeRequest xmlns="Alma/pipelinescience/SciPipeRequest"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="SciPipeRequest">
    <SciPipeRequestEntity entityId="UID_UNASSIGNED"
        entityTypeName="SciPipeRequest" datamodelVersion="0.1"/>
    <ProjectSummary>
        <ProposalCode xmlns="">1234.5.67890.S</ProposalCode>
        <Observatory xmlns="">ALMA Joint Observatory</Observatory>
        <Telescope xmlns="">ALMA</Telescope>
        <ProcessingSite xmlns="">Undefined</ProcessingSite>
        <Operator xmlns="">Anonymous</Operator>
        <Mode xmlns="">CSV</Mode>
        <Version xmlns="">Undefined</Version>
        <CreationTime xmlns="">1970-01-01T00:00:00.000</CreationTime>
    </ProjectSummary>
    <ProjectStructure>
        <AlmaStructure>
            <ns1:ObsUnitSetRef xmlns:ns1="Alma/ObsPrep/ObsProject"
                entityId="uid://A001/X12/X345" partId="X23868574" entityTypeName="ObsProject"/>
            <ObsUnitSetTitle xmlns="">Undefined</ObsUnitSetTitle>
            <ObsUnitSetType xmlns="">Member</ObsUnitSetType>
            <ns2:ProjectStatusRef
                xmlns:ns2="Alma/Scheduling/ProjectStatus"
                entityId="uid://A001/X23/X456" entityTypeName="ProjectStatus"/>
            <ns3:OUSStatusRef xmlns:ns3="Alma/Scheduling/OUSStatus"
                entityId="uid://A001/X34/X567" entityTypeName="OUSStatus"/>
        </AlmaStructure>
    </ProjectStructure>
    <ProcessingRequests>
        <ProcessingRequest>
            <ProcessingIntents>
                <Intents>
                    <Keyword xmlns="">PROCESS</Keyword>
                    <Value xmlns="">true</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">SESSION_1</Keyword>
                    <Value xmlns="">uid://A002/X123456/X78</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">SESSION_2</Keyword>
                    <Value xmlns="">uid://A002/X234567/X89</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">SESSION_3</Keyword>
                    <Value xmlns="">uid://A002/X345678/X90</Value>
                </Intents>
                <Intents>
                    <Keyword xmlns="">INTERFEROMETRY_STANDARD_OBSERVING_MODE</Keyword>
                    <Value xmlns="">Undefined</Value>
                </Intents>
            </ProcessingIntents>
            <ProcessingProcedure>
                <ProcessingCommand>
                <Command>test_parameter_cast</Command>
                <ParameterSet>
                    <Parameter>
                    <Keyword>parameter</Keyword>
                    <Value>{:s}</Value>
                    </Parameter>
                    <Parameter>
                    <Keyword>expect_tuple</Keyword>
                    <Value>(1,2,3)</Value>
                    </Parameter>
                </ParameterSet>
                </ProcessingCommand>
            </ProcessingProcedure>
            <DataSet>
                <SchedBlockSet>
                    <SchedBlockIdentifier>
                        <RelativePath xmlns="">..</RelativePath>
                        <ns4:SchedBlockRef
                            xmlns:ns4="Alma/ObsPrep/SchedBlock"
                            entityId="uid://A001/X12/X34"
                            entityTypeName="SchedBlock" documentVersion="1"/>
                        <ns5:SBStatusRef
                            xmlns:ns5="Alma/Scheduling/SBStatus"
                            entityId="uid://A001/X12/X35" entityTypeName="SBStatus"/>
                        <SBTitle xmlns="">Undefined</SBTitle>
                        <AsdmIdentifier>
                            <ns7:AsdmRef xmlns:ns7="Alma/ValueTypes">
                                <ns7:ExecBlockId>uid://A002/X30a93d/X43e</ns7:ExecBlockId>
                            </ns7:AsdmRef>
                            <AsdmDiskName xmlns="">uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms</AsdmDiskName>
                        </AsdmIdentifier>
                    </SchedBlockIdentifier>
                </SchedBlockSet>
            </DataSet>
        </ProcessingRequest>
    </ProcessingRequests>
    <ResultsProcessing>
        <ArchiveResults xmlns="">false</ArchiveResults>
        <CleanUpDisk xmlns="">false</CleanUpDisk>
        <UpdateProjectLifeCycle xmlns="">false</UpdateProjectLifeCycle>
        <NotifyOperatorWhenDone xmlns="">false</NotifyOperatorWhenDone>
        <PipelineOperatorAdress xmlns="">Unknown</PipelineOperatorAdress>
    </ResultsProcessing>
</SciPipeRequest>
"""
    return StringIO(template_ppr.format(input_parameter_str))


@pytest.mark.parametrize("input_str, expected_value", test_parameter_values)
def test_xmlobjectifier_casttype(input_str, expected_value):
    """Test the parameter value paraser from pipeline.external.XmlObjectifier."""
    info, structure, relativePath, intentsDict, asdmList, procedureName, commandsList = _getFirstRequest(
        create_example_alma_ppr(input_str))
    assert commandsList[0][1]['parameter'] == expected_value
