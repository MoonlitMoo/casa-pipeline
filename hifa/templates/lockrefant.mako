<%!
    import os
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Lock refant list</%block>

<h2>Results</h2>

<p>The reference antenna list is now frozen. The reference antenna list cannot be modified by any subsequent task unless
    the list is unfrozen with hifa_unlock_refant. The frozen reference antenna list for each measurement set is listed
    below</p>
<p>All subsequent gaincal calls will be executed with refantmode='strict' unless an override refantmode parameter is
    provided.</p>

<table class="table table-bordered table-striped"
       summary="Reference Antennas">
    <caption>Reference antenna selection per measurement set. Antennas are listed in order of highest to lowest
        priority.
    </caption>
    <thead>
    <tr>
        <th>Measurement Set</th>
        <th>Reference Antennas (Highest to Lowest)</th>
    </tr>
    </thead>
    <tbody>
        %for tr in refant_table:
            <tr>
                % for td in tr:
                ${td}
                % endfor
            </tr>
        % endfor
    </tbody>
</table>