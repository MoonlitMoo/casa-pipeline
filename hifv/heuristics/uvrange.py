def uvrange(setjy_results, field_id, spw_id=2):
    """

    Args:
        setjy_results: Flux domain object read in from the import stage
        field_id: integer field id
        spw_id: integer spw id, default is spw_id=2 for VLASS
        ** However, currently it just picks the first index of zero

    uvmin and uvmax are of type Decimal

    Units are always assumed to be lambda

    Returns: uvrange string

    """

    try:
        # spw_index = [flux.spw_id for flux in setjy_results[0].measurements[field_id]].index(spw_id)
        spw_index = 0

        uvmin = setjy_results[0].measurements[field_id][spw_index].uvmin
        uvmax = setjy_results[0].measurements[field_id][spw_index].uvmax
    except Exception as e:
        uvmin = 0.0
        uvmax = 0.0

    if float(uvmin) == 0.0 and float(uvmax) == 0.0:
        return ''

    if float(uvmin) != 0.0 and float(uvmax) == 0.0:
        return '>{!s}lambda'.format(str(float(uvmin)))

    if float(uvmin) != 0.0 and float(uvmax) != 0.0:
        return '{!s}~{!s}lambda'.format(str(float(uvmin)), str(float(uvmax)))
