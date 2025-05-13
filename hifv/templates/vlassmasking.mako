<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Vlassmasking</%block>

% for single_result in result:
    % if single_result.inputs['maskingmode'] == 'vlass-se-tier-1':
        <table class="table table-bordered table-striped table-condensed" summary="Catalogs and masks width="60%">
        <tbody>
        <tr><td width="30%">Catalog             </td><td><b>${single_result.catalog_fits_file}</b></td></tr>
        <tr><td width="30%">Catalog search size </td><td><b>${single_result.catalog_search_size} degrees</b></td></tr>
        <tr><td width="30%">Tier 1 Mask         </td><td><b>${single_result.tier1mask}</b></td></tr>
        </tbody>
        </table>

        <table class="table table-bordered table-striped table-condensed" summary="New islands found" width="60%">
        <tbody>
        <tr><td width="30%">Number of new islands found </td><td><b>${single_result.number_islands_found}</b></td></tr>
        <tr><td width="30%">Number of new islands found (inner square degree) </td><td><b>${single_result.number_islands_found_onedeg}</b></td></tr>
        </tbody>
        </table>

        <table class="table table-bordered table-striped table-condensed" summary="New islands found" width="60%">
        <tbody>
        <tr><td width="30%">Fraction of pixels enclosed in the tier-1 mask </td><td><b>${"{:.{}f}".format(single_result.pixelfractions['tier1'], 8)}</b></td></tr>
        <tr><td width="30%">Fraction of pixels enclosed in the tier-1 mask (inner square degree) </td><td><b>${"{:.{}f}".format(single_result.pixelfractions['tier1_onedeg'], 8)}</b></td></tr>
        </tbody>
        </table>

    % elif single_result.inputs['maskingmode'] == 'vlass-se-tier-2':

        <table class="table table-bordered table-striped table-condensed" summary="Catalogs and masks" width="60%">
        <tbody>
        <tr><td width="30%">Catalog </td><td> <b>${single_result.catalog_fits_file}</b></td></tr>
        <tr><td width="30%">Catalog search size </td><td> <b>${single_result.catalog_search_size} degrees</b></td></tr>
        <tr><td width="30%">Tier 1 Mask </td><td> <b>${single_result.tier1mask}</b></td></tr>
        <tr><td width="30%">Tier 2 Mask </td><td> <b>${single_result.tier2mask}</b></td></tr>
        <tr><td width="30%">Combined Final Mask </td><td> <b>${single_result.combinedmask}</b></td></tr>
        </tbody>
        </table>

        <table class="table table-bordered table-striped table-condensed" summary="New islands found" width="60%">
        <tbody>
        <tr><td width="30%">Number of new islands found </td><td> <b>${single_result.number_islands_found}</b></td></tr>
        <tr><td width="30%">Number of new islands found (inner square degree) </td><td> <b>${single_result.number_islands_found_onedeg}</b></td></tr>
        <tr><td width="30%">Number of new islands rejected (tier-2) </td><td> <b>${single_result.num_rejected_islands}</b></td></tr>
        <tr><td width="30%">Number of new islands rejected (tier-2, inner square degree) </td><td> <b>${single_result.num_rejected_islands_onedeg}</b></td></tr>
        </tbody>
        </table>

        <table class="table table-bordered table-striped table-condensed" summary="Pixel fractions enclosed" width="60%">
        <tbody>
        <tr><td width="30%">Fraction of pixels enclosed in the tier-1 mask </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['tier1'], 8)}</b></td></tr>
        <tr><td width="30%">Fraction of pixels enclosed in the tier-1 mask (inner square degree) </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['tier1_onedeg'], 8)}</b></td></tr>

        <tr><td width="30%">Fraction of pixels enclosed in the tier-2 mask </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['tier2'], 8)}</b></td></tr>
        <tr><td width="30%">Fraction of pixels enclosed in the tier-2 mask (inner square degree) </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['tier2_onedeg'], 8)}</b></td></tr>

        <tr><td width="30%">Fraction of pixels enclosed in the final combined mask </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['final'], 8)}</b></td></tr>
        <tr><td width="30%">Fraction of pixels enclosed in the final combined mask (inner square degree) </td><td> <b>${"{:.{}f}".format(single_result.pixelfractions['final_onedeg'], 8)}</b></td></tr>
        </tbody>
        </table>

        <table class="table table-bordered table-striped table-condensed" summary="Increase in masked pixels" width="60%">
        <tbody>
        <tr><td width="30%">Fractional increase of masked pixels in final combined mask relative to Quicklook Mask </td><td> <b>${"{:.{}f}".format(single_result.relativefraction*100.0, 2)}% increase</b></td></tr>
        <tr><td width="30%"> Fractional increase of masked pixels in final combined mask relative to Quicklook Mask (inner square degree) </td><td> <b>${"{:.{}f}".format(single_result.relativefraction_onedeg*100.0, 2)}% increase</b></td></tr>
        </tbody>
        </table>

    % else:
        <p>Incompatible mode used in maskingmode input</p>
    % endif



    <%self:plot_group plot_dict="${summary_plots}" url_fn="${lambda ms:  'noop'}">

            <%def name="title()">
                Summary plot for ${single_result.plotmask}
            </%def>

            <%def name="preamble()">

            </%def>

            <%def name="mouseover(plot)">
                Summary window
            </%def>

            <%def name="fancybox_caption(plot)">
              ${single_result.plotmask}
            </%def>

            <%def name="caption_title(plot)">
               ${single_result.plotmask}
            </%def>
    </%self:plot_group>

% endfor