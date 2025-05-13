"""Observatory policy for single-dish imaging."""
import abc
from typing import Type

import numpy as np

import casatasks.private.sdbeamutil as sdbeamutil

from pipeline.domain import MeasurementSet
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
from pipeline.infrastructure.launcher import Context
from . import utils as sdutils


LOG = infrastructure.get_logger(__name__)


class ObservatoryImagingPolicy(abc.ABC):
    """Base class for observatory imaging policy."""

    @staticmethod
    @abc.abstractmethod
    def get_beam_size_arcsec(ms: MeasurementSet, spw_id: int) -> float:
        """Get beam size in arcsec.

        Args:
            ms: MS domain object
            spw_id: Spectral window id

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) Beam size in arcsec.
        """
        raise NotImplementedError('should be implemented in subclass')

    @staticmethod
    @abc.abstractmethod
    def get_beam_size_pixel() -> int:
        """Get beam size as number of pixels.

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) Beam size as number of pixels.
        """
        raise NotImplementedError('should be implemented in subclass')

    @staticmethod
    @abc.abstractmethod
    def get_convsupport() -> int:
        """Get convolution support.

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) Convolution support.
        """
        raise NotImplementedError('should be implemented in subclass')

    @staticmethod
    @abc.abstractmethod
    def get_image_margin() -> int:
        """Get image margin in pixel numbers.

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) number of pixels.
        """
        raise NotImplementedError('should be implemented in subclass')

    @staticmethod
    @abc.abstractmethod
    def get_conv1d() -> float:
        """Get the constant of conv1d. This is to obtain convolution factors in sdimaging task.

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) the constant of conv1d.
        """
        raise NotImplementedError('should be implemented in subclass')

    @staticmethod
    @abc.abstractmethod
    def get_conv2d() -> float:
        """Get the constant of conv2d. This is to obtain convolution factors in sdimaging task.

        Raises:
            NotImplementedError

        Returns:
            (Supposed to be) the constant of conv2d.
        """
        raise NotImplementedError('should be implemented in subclass')


class ALMAImagingPolicy(ObservatoryImagingPolicy):
    """Implementation of imaging policy for ALMA."""

    @staticmethod
    def get_beam_size_arcsec(ms: MeasurementSet, spw_id: int) -> float:
        """Get beam size in arcsec.

        Args:
            ms: MS domain object
            spw_id: Spectral window id

        Returns:
            Beam size in arcsec.
        """
        # recommendation by EOC
        fwhmfactor = 1.13
        # hard-coded for ALMA-TP array
        diameter_m = 12.0
        obscure_alma = 0.75

        spw = ms.get_spectral_window(spw_id)
        freq_hz = np.float64(spw.mean_frequency.value)

        theory_beam_arcsec = sdbeamutil.primaryBeamArcsec(
            freq_hz,
            diameter_m,
            obscure_alma,
            10.0,
            fwhmfactor=fwhmfactor
        )

        return theory_beam_arcsec

    @staticmethod
    def get_beam_size_pixel() -> int:
        """Get beam size as number of pixels.

        Returns:
            Beam size as number of pixels.
        """
        return 9

    @staticmethod
    def get_convsupport() -> int:
        """Get convolution support.

        Returns:
            Convolution support.
        """
        return 6

    @staticmethod
    def get_image_margin() -> int:
        """Get image margin in pixel numbers.

        Returns:
            number of pixels of imaging margin (adjusted to even number)
        """
        margin = ALMAImagingPolicy.get_beam_size_pixel()
        margin += margin % 2
        return margin

    @staticmethod
    def get_conv1d() -> float:
        """Get the constant of conv1d. This is to obtain convolution factors in sdimaging task.

        Returns:
            the constant of conv1d.
        """
        return 0.3954

    @staticmethod
    def get_conv2d() -> float:
        """Get the constant of conv2d. This is to obtain convolution factors in sdimaging task.

        Returns:
            the constant of conv2d.
        """
        return 0.1597

class NROImagingPolicy(ObservatoryImagingPolicy):
    """Implementation of imaging policy for NRO 45m telescope."""

    @staticmethod
    def get_beam_size_arcsec(ms: MeasurementSet, spw_id: int) -> float:
        """Get beam size in arcsec.

        Args:
            ms: MS domain object
            spw_id: Spectral window id

        Returns:
            Beam size in arcsec.
        """
        qa = casa_tools.quanta
        # ms.beam_sizes is a nested dictionary with
        # antenna_id and spw_id as the keys:
        #
        # ms.beam_sizes = {antenna_id: {spw_id: beam_size}}
        beam_sizes = [b[spw_id] for b in ms.beam_sizes.values() if spw_id in b]
        beam_size = np.mean([qa.convert(b, 'arcsec')['value'] for b in beam_sizes])
        return beam_size

    @staticmethod
    def get_beam_size_pixel() -> int:
        """Get beam size as number of pixels.

        Returns:
            Beam size as number of pixels.
        """
        return 3

    @staticmethod
    def get_convsupport() -> int:
        """Get convolution support.

        Returns:
            Convolution support.
        """
        return 3

    @staticmethod
    def get_image_margin() -> int:
        """Get image margin in pixel numbers.

        Returns:
            number of pixels of imaging margin
        """
        return 0

    @staticmethod
    def get_conv1d() -> float:
        """Get the constant of conv1d. This is to obtain convolution factors in sdimaging task.

        Returns:
            the constant of conv1d.
        """
        return 0.5592

    @staticmethod
    def get_conv2d() -> float:
        """Get the constant of conv2d. This is to obtain convolution factors in sdimaging task.

        Returns:
            the constant of conv2d.
        """
        return 0.3193


def get_imaging_policy(context: Context) -> Type[ObservatoryImagingPolicy]:
    """Get appropriate observatory policy for imaging.

    Args:
        context: Pipeline context.

    Returns:
        One of the subclass of ObservatoryImagingPolicy.
    """
    is_nro = sdutils.is_nro(context)
    if is_nro:
        return NROImagingPolicy
    else:
        return ALMAImagingPolicy
