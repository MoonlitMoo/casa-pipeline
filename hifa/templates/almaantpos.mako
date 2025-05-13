<%!
rsc_path = ""
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Calibrate Antenna Positions</%block>

<h2>Results</h2>

% if not table_rows:
    <p>No antenna positions were corrected.</p>
% else:
    <h4>Antenna Position Offsets</h4>

    <p>The following antenna position x, y, z offsets were used to calibrate the antenna positions. Values above a threshold of ${"%0.2f" % threshold_in_wavelengths} wavelengths = ${"%0.2f" % threshold_in_mm} mm are bolded.</p>

    <table class="table table-bordered table-striped" summary="Antenna Position Offsets">
        <caption>Antenna position offsets (sorted by antenna name) per measurement set.</caption>
        <thead>
            <tr>
            <th scope="col">Measurement Set</th>
            <th scope="col">Antenna</th>
            <th scope="col">X Offset (m)</th>
            <th scope="col">Y Offset (m)</th>
            <th scope="col">Z Offset (m)</th>
            <th scope="col">Total Offset (mm)</th>
            <th scope="col">Total Offset (wavelength)</th>
        </tr>
        </thead>
        <tbody>
        % for tr in table_rows:
        ## Bold table rows where the total offset is greater than a threshold. See PIPE-77.
        <%
            import re
            comparison = re.sub(r"<.*?>", "", tr[-2]).strip()
            if comparison != '' :
                comparison=float(comparison)
        %>
            %if comparison != '' and comparison > threshold_in_mm:
                <tr style="font-weight:bold">
            %else:
                <tr>
            %endif
            % for td in tr:
                ${td}
            % endfor
        </tr>
        % endfor
        </tbody>
    </table>

    <h4>Antenna Position Offsets Sorted By Total Offset</h4>

    <p>The following antenna position x, y, z offsets were used to calibrate the antenna positions. Values above a threshold of ${"%0.2f" % threshold_in_wavelengths} wavelengths = ${"%0.2f" % threshold_in_mm} mm are bolded.</p>

    <table class="table table-bordered table-striped" summary="Antenna Position Offsets">
        <caption>Antenna position offsets (sorted by total offset) per measurement set.</caption>
        <thead>
            <tr>
            <th scope="col">Measurement Set</th>
            <th scope="col">Antenna</th>
            <th scope="col">X Offset(m)</th>
            <th scope="col">Y Offset(m)</th>
            <th scope="col">Z Offset (m)</th>
            <th scope="col">Total Offset (mm)</th>
            <th scope="col">Total Offset (wavelength)</th>
        </tr>
        </thead>
        <tbody>
        % for tr in table_rows_by_offset:
        ## Bold table rows where the total offset is greater than a threshold. See PIPE-77.
        <%
            import re
            comparison = re.sub(r"<.*?>", "", tr[-2]).strip()
            if comparison != '' :
                comparison=float(comparison)
        %>
            %if comparison != '' and comparison > threshold_in_mm:
                <tr style="font-weight:bold">
            %else:
                <tr>
            %endif

            % for td in tr:
                ${td}
            % endfor
        </tr>
        % endfor
        </tbody>
    </table>

% endif
