<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr

def num_targets (result):
    ntargets = 0
    for r in result:
        ntargets = ntargets + len(r.targets)
    return ntargets

def no_clean_targets(result):
    no_clean_targets = True
    for r in result:
        if r.clean_list_info == {}:
            continue
        no_clean_targets = False
        break
    return no_clean_targets

def get_message(result):
    message = ""
    for r in result:
        message = r.clean_list_info.get('msg', '')
        break
    return message

def wrap_long_spw_str(spw_str, length=50):
    '''Wraps long string by inserting new line escape sequence(s).

    :param spw_str: comma separated list of spectral window ids (string)
    :param length: maximum number of characters in a text line (integer)
    '''
    if len(spw_str) > length:
        spwlist = spw_str.split(',')
        # spw id may have more digits and comas also contributes to the string length limit
        # If the modulo of the string created from the first n element is larger than that of the first n+1 element,
        # then the line length limit is reached and a linebreak is inserted.
        wrapped_spw_str = ['%s' % spwlist[i] if len(','.join(spwlist[0:i])) % length <
                           len(','.join(spwlist[0:i+1])) % length else '\n%s' % spwlist[i]
                           for i in range(len(spwlist)-1)] + [spwlist[-1]]
        return ','.join(wrapped_spw_str)
    else:
        return spw_str

def check_all_targets(result_obj, key, none_values):
    '''
    Check if a given ket exists in any of the imaging targets of
    a given result list and is not set to a none value.

    :param result_obj: hif_makeimlist result object
    :param key: dictionary key to check
    :param none_values: list of none values for the given key
    '''

    return any([target.get(key, None) not in none_values + [None] for r in result_obj for target in r.targets])
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Make image list<br><small>${result[0].metadata['long description']}</small></%block>

<h2>List of Clean Targets</h2>

%if result[0].error:
This task had an error!
%elif num_targets(result) <= 0:
    %if no_clean_targets(result):
        <p>There are no clean targets.
    %else:
        <p>${get_message(result)}
    %endif
%else:
    <p>${get_message(result)}

    <table class="table table-bordered table-striped">
                <caption>Clean Targets Summary</caption>
                <thead>
                <tr>
                    <th>field</th>
                    <th>intent</th>
                    <th>spw</th>
                    <th>data type</th>
                    <th>phasecenter</th>
                    <th>cell</th>
                    <th>imsize</th>
                    <th>imagename</th>
                %if check_all_targets(result, 'specmode', ['', None]):
                    <th>specmode</th>
                %endif
                %if check_all_targets(result, 'start', ['', None]):
                    <th>start</th>
                %endif
                %if check_all_targets(result, 'width', ['', None]):
                    <th>width</th>
                %endif
                %if check_all_targets(result, 'nbin', [-1, None]):
                    <th>nbin</th>
                %endif
                %if check_all_targets(result, 'nchan', [-1, None]):
                    <th>nchan</th>
                %endif
                %if check_all_targets(result, 'restfreq', ['', None]):
                    <th>restfreq (LSRK)</th>
                %endif
                %if check_all_targets(result, 'weighting', ['', None]):
                    <th>weighting</th>
                %endif
                %if check_all_targets(result, 'robust', [-999, None]):
                    <th>robust</th>
                %endif
                %if check_all_targets(result, 'noise', ['', None]):
                    <th>noise</th>
                %endif
                %if check_all_targets(result, 'npixels', [-1, None]):
                    <th>npixels</th>
                %endif
                %if check_all_targets(result, 'restoringbeam', ['', None]):
                    <th>restoringbeam</th>
                %endif
                %if check_all_targets(result, 'uvrange', ['', None]):
                    <th>uvrange</th>
                %endif
                %if check_all_targets(result, 'maxthreshiter', [-1, '', None]):
                    <th>maxthreshiter</th>
                %endif
                </tr>
                </thead>
                <tbody>
        %for r in result:
            %for target in r.targets:
                <tr>
                    <td>${target['field']}</td>
                    <td>${target['intent']}</td>
                    <td>${wrap_long_spw_str(target['spw'])}</td>
                    <td>${target['datatype_info']}</td>
                    <td>${target['phasecenter']}</td>
                    <td>${target['cell']}</td>
                    <td>${target['imsize']}</td>
                    <td>${os.path.basename(target['imagename'])}</td>
                %if check_all_targets(result, 'specmode', ['', None]):
                    %if target.get('specmode', None) not in ['', None]:
                        <td>${target['specmode']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'start', ['', None]):
                    %if target.get('start', None) not in ['', None]:
                        <td>${target['start']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'width', ['', None]):
                    %if target.get('width', None) not in ['', None]:
                        <td>${target['width']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'nbin', [-1, None]):
                    %if target.get('nbin', None) not in [-1, None]:
                        <td>${target['nbin']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'nchan', [-1, None]):
                    %if target.get('nchan', None) not in [-1, None]:
                        <td>${target['nchan']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'restfreq', ['', None]):
                    %if target.get('restfreq', None) not in ['', None]:
                        <td>${target['restfreq']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'weighting', ['', None]):
                    %if target.get('weighting', None) not in ['', None]:
                        <td>${target['weighting']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'robust', [-999, None]):
                    %if target.get('robust', None) not in [-999, None]:
                        <td>${target['robust']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'noise', ['', None]):
                    %if target.get('noise', None) not in ['', None]:
                        <td>${target['noise']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'npixels', [-1, None]):
                    %if target.get('npixels', None) not in [-1, None]:
                        <td>${target['npixels']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'restoringbeam', ['', None]):
                    %if target.get('restoringbeam', None) not in ['', None]:
                        <td>${target['restoringbeam']}<td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'uvrange', ['', None]):
                    %if target.get('uvrange', None) not in ['', None]:
                        <td>${target['uvrange']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                %if check_all_targets(result, 'maxthreshiter', [-1, '', None]):
                    %if target.get('maxthreshiter', None) not in [-1, '', None]:
                        <td>${target['maxthreshiter']}</td>
                    %else:
                        <td>None</td>
                    %endif
                %endif
                        </tr>
            %endfor
        %endfor
                </tbody>
        </table>
%endif
