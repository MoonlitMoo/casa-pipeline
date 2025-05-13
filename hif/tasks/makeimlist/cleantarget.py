class CleanTarget(dict):
    """Clean target template definition."""

    def __init__(self, *args, **kwargs):
        self['antenna'] = None              # list of strings
        self['field'] = None                # string
        self['intent'] = None               # string
        self['spw'] = None                  # string
        self['spwsel_lsrk'] = None          # dictionary
        self['spwsel_topo'] = None          # list
        self['spwsel_all_cont'] = None      # boolean
        self['spwsel_low_bandwidth'] = None # boolean
        self['spwsel_low_spread'] = None    # boolean
        self['num_all_spws'] = None         # int
        self['num_good_spws'] = None        # int
        self['cell'] = None                 # string
        self['cfcache'] = None              # string
        self['imsize'] = None               # string / list
        self['pblimit'] = None              # float
        self['phasecenter'] = None          # string
        self['psf_phasecenter'] = None      # string
        self['specmode'] = None             # string
        self['gridder'] = None              # string
        self['wprojplanes'] = None          # int
        self['datacolumn'] = None           # string
        self['datatype'] = None             # string
        self['datatype_info'] = None        # string
        self['deconvolver'] = None          # string
        self['imagename'] = None            # string
        self['start'] = None                # string
        self['width'] = None                # string
        self['nbin'] = None                 # int
        self['nchan'] = None                # int
        self['stokes'] = None               # string
        self['nterms'] = None               # int
        self['robust'] = None               # float
        self['uvrange'] = None              # string / list
        self['bl_ratio'] = None             # float
        self['uvtaper'] = None              # list
        self['scales'] = None               # list
        self['niter'] = None                # int
        self['cycleniter'] = None           # int
        self['cyclefactor'] = None          # float
        self['nmajor'] = None               # int
        self['sensitivity'] = None          # string
        self['threshold'] = None            # string
        self['reffreq'] = None              # string
        self['restfreq'] = None             # string
        self['heuristics'] = None           # object
        self['vis'] = None                  # list of strings
        self['is_per_eb'] = None            # boolean
        self['usepointing'] = None          # boolean
        self['mosweight'] = None            # boolean
        self['drcorrect'] = None            # float
        self['flagpct'] = None              # float

        dict.__init__(self, *args, **kwargs)


class ScalTarget(CleanTarget):
    """Selfcal target template definition."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['sc_band'] = None              # string
        self['sc_solints'] = None           # list
        self['sc_lib'] = None               # dictionary
        self['sc_workdir'] = None           # string
        self['sc_vislist'] = None           # input vis list
        self['sc_telescope'] = None         # string
        self['sc_parallel'] = None          # string
        self['sc_field']= None              # string, dequoted, alias of field_name
        self['field_name'] = None           # string, dequoted
        self['field_id'] = None             # int
        self['spw_real'] = None             # spw_real lookup table
