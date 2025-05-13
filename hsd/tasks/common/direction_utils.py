"""
direction_utils.py: methods to convert coordinates for ephemeris sources.

Methods to convert coordinates of ephemeris sources for single-dish pipeline
are accumulated here.
For SD ephemeris sources, two types of coordinates are introduced:
'Shifted-direction' and 'Offset-direction'.
Both coodinates deal with time-by-time observing direction 
when an ephemeris source is scanned (typically with raster scan).
'Offset-direction' is a coordinate-system where the time-by-time direction 
is shifted so that the source position is centered at the origin (0 ,0)
of the RA/Dec plane. In other words, diretions of each observing point 
during the scan are calculated as the offset from the ephemeris source.
'Shifted-direction' is a coordinate-system where 
the time-by-time direction is shifted so that the source position 
is centered at 'origin' (or org_direction), 
which is where the epheris source resided on the RA/Dec plane 
at the time of the first on-source observing point in the dataset. 
"""

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

from typing import Dict, NewType, Tuple, Union

Quantity  = NewType( 'Quantity',  Dict )
Direction = NewType( 'Direction', Dict )
Epoch     = NewType( 'Epoch',     Dict )
Position  = NewType( 'Position',  Dict )

__all__ = { 'direction_shift', 'direction_offset', 'direction_recover', 'direction_convert' }


def direction_shift( direction:Direction, reference:Direction, origin:Direction ) -> Direction:
    """
    Calculate the 'shifted-direction' of the observing point.

    This method calculates the 'shifted-direction' of the observing point
    from the time-by-time diretion of obsering points and the moving source,
    and a given 'origin'. 

    Args:
        direction: direction to be converted  
                   (eg. time-by-time position of observing points)
        reference: reference direction 
                   (eg. time-by-time position of the moving source on the sky)
        origin:    direction of the origin 
                   (eg. where to centerized the new image)           
    Returns:                           
        shifted-direction (reference centerized at origin)
    Raises:
        RunTimeRrror: If 'refer's are inconsistent among direction, reference, and origin
    """
    # check if 'refer's are all identical for each directions
    if origin['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and origin should be identical" )
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casa_tools.measures
    offset = me.separation( reference, origin )
    posang = me.posangle( reference, origin )
    new_direction = me.shift( direction, offset=offset, pa=posang )

    return new_direction


def direction_offset( direction:Direction, reference:Direction ) -> Direction:
    """
    Calculate the 'offset-direction' of the observing point.

    This method calculates the 'offset-direction' of the observing point 
    from the time-by-time diretion of obsering points and the moving source.
    This is equivallent to calling 
    direction_shift( direction, reference, origin ) with 
    the coordinate-origin (0, 0) as 'origin'.

    Args:
        direction: direction to be converted  
                   (eg. time-by-time position of observing points)
        reference: reference direction 
                   (eg. time-by-time position of the moving source on the sky)
    Returns:                           
        offset-direction (reference centerized at (0,0) )
    Raises:
        RunTimeError: If 'refer's of directoin and reference are inconsistent
    """
    # check if 'refer's are all identical for each directions
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casa_tools.measures
    offset = me.separation( reference, direction )
    posang = me.posangle( reference, direction )

    outref = direction['refer']
    zero_direction = me.direction( outref, '0.0deg', '0.0deg' )
    new_direction = me.shift( zero_direction, offset=offset, pa=posang )

    return new_direction


def direction_recover( ra:float, dec:float, org_direction:Direction ) -> Tuple[float, float]:
    """
    Recovers the 'Shifted-coordinate' from 'Offset-coordinate'.

    Recovers the 'Shifted-coordinate' values from the specified 
    'Offset-coordinate' values.

    Args:
        ra:  ra of 'Offset-corrdinate'
        dec: dec of 'Offset-coordinate'
        org_direction: direction of the origin
    Returns:                                                               
        return value: ra, dec in 'Shift-coordinate'
    """
    me = casa_tools.measures
    qa = casa_tools.quanta

    direction = me.direction( org_direction['refer'],
                              str(ra)+'deg', str(dec)+'deg' )
    zero_direction  = me.direction( org_direction['refer'], '0deg', '0deg' )
    offset = me.separation( zero_direction, direction )
    posang = me.posangle( zero_direction, direction )
    new_direction = me.shift( org_direction, offset=offset, pa=posang )
    new_ra  = qa.convert( new_direction['m0'], 'deg' )['value']
    new_dec = qa.convert( new_direction['m1'], 'deg' )['value']

    return new_ra, new_dec


def direction_convert(direction:Direction, mepoch:Epoch, mposition:Position, outframe:str) -> Tuple[Quantity, Quantity]:
    """
    Convert the frame of the 'direction' to 'outframe'.

    Convert the 'frame' of the direction to that specified as 'outframe'.
    If 'outframe' is identical to the frame of the 'direction', 
    the original 'direction' will be returned.

    Args:
        direction:  original direction
        mepoch:     epoch
        mposition:  position
        outframe:   frame of output direction
    Returns:
        return values
    """
    direction_type = direction['type']
    assert direction_type == 'direction'
    inframe = direction['refer']

    # if outframe is same as input direction reference, just return
    # direction as it is
    if outframe == inframe:
        # return direction
        return direction['m0'], direction['m1']

    # conversion using measures tool
    me = casa_tools.measures
    me.doframe(mepoch)
    me.doframe(mposition)
    out_direction = me.measure(direction, outframe)
    return out_direction['m0'], out_direction['m1']
