<%!
    import os
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Unlock refant list</%block>

<h2>Results</h2>

<p>The reference antenna list can now be modified by subsequent tasks. All subsequent gaincal calls will be executed
    with refantmode='flex' unless an override refantmode parameter is provided.</p>
