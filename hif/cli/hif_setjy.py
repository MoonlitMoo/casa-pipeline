import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_setjy(vis=None, field=None, intent=None, spw=None, model=None,
              reffile=None, normfluxes=None, reffreq=None, fluxdensity=None,
              spix=None, scalebychan=None, standard=None):

    """
    hif_setjy ---- Fill the model column with calibrated visibilities


    Fills the model column with the model visibilities.


    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets defined in the pipeline context.
    field         The list of field names or field ids for which the models are
                  to be set. Defaults to all fields with intent '`*AMPLITUDE*`'.

                  example: field='3C279', field='3C279, M82'
    intent        A string containing a comma delimited list of intents against
                  which the selected fields are matched. Defaults to all data
                  with amplitude intent.

                  example: intent='`*AMPLITUDE*`'
    spw           The list of spectral windows and channels for which bandpasses are
                  computed. Defaults to all science spectral windows.

                  example: spw='11,13,15,17'
    model         Model image for setting model visibilities. Not fully
                  supported.

                  example: see details in help for CASA setjy task
    reffile       Path to a file containing flux densities for calibrators unknown to
                  CASA. Values given in this file take precedence over the CASA-derived
                  values for all calibrators except solar system calibrators. By default the
                  path is set to the CSV file created by h_importdata, consisting of
                  catalogue fluxes extracted from the ASDM.

                  example: reffile='', reffile='working/flux.csv'
    normfluxes    Normalize lookup fluxes.
    reffreq       The reference frequency for spix, given with units. Provided to
                  avoid division by zero. If the flux density is being scaled by spectral
                  index, then reffreq must be set to whatever reference frequency is correct
                  for the given fluxdensity and spix. It cannot be determined from vis. On
                  the other hand, if spix is 0, then any positive frequency can be used and
                  will be ignored.

                  example: reffreq='86.0GHz', reffreq='4.65e9Hz'
    fluxdensity   Specified flux density [I,Q,U,V] in Jy. Uses [1,0,0,0]
                  flux density for unrecognized sources, and standard flux densities for
                  ones recognized by 'standard', including 3C286, 3C48, 3C147, and several
                  planets, moons, and asteroids.

                  example: [3.06,0.0,0.0,0.0]
    spix          Spectral index for fluxdensity S = fluxdensity * (freq/reffreq)**spix
                  Only used if fluxdensity is being used. If fluxdensity is positive, and
                  spix is nonzero, then reffreq must be set too. It is applied in the same
                  way to all polarizations, and does not account for Faraday rotation or
                  depolarization.
    scalebychan   This determines whether the fluxdensity set in the model is
                  calculated on a per channel basis. If False then only one fluxdensity
                  value is calculated per spw.
    standard      Flux density standard, used if fluxdensity[0] less than 0.0. The
                  options are: 'Baars','Perley 90','Perley-Taylor 95', 'Perley-Taylor 99',
                  'Perley-Butler 2010' and 'Butler-JPL-Horizons 2010'.

                  default: 'Butler-JPL-Horizons 2012' for solar system object
                                           'Perley-Butler 2010' otherwise

    --------- examples -----------------------------------------------------------


    1. Set the model flux densities for all the amplitude calibrators:

    >>> hif_setjy()

    --------- issues -----------------------------------------------------------

    Support for the setjy spix parameter needs to be added.

    """


    ##########################################################################
    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #
    ##########################################################################

    # create a dictionary containing all the arguments given in the
    # constructor
    all_inputs = vars()

    # get the name of this function for the weblog, eg. 'hif_flagdata'
    task_name = sys._getframe().f_code.co_name

    # get the context on which this task operates
    context = utils.get_context()

    # execute the task
    results = utils.execute_task(context, task_name, all_inputs)

    return results
