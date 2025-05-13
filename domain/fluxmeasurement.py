from decimal import Decimal

import pipeline.infrastructure as infrastructure
from .measures import FluxDensity, FluxDensityUnits

LOG = infrastructure.get_logger(__name__)


class FluxMeasurement(object):
    def __init__(self, spw_id, I, Q=FluxDensity(0), U=FluxDensity(0), V=FluxDensity(0), spix=Decimal('0.0'),
                 uvmin=Decimal('0.0'), uvmax=Decimal('0.0'),
                 origin=None, age=None, queried_at=None):
        self.spw_id = int(spw_id)
        self.I = self._to_flux_density(I)
        self.Q = self._to_flux_density(Q)
        self.U = self._to_flux_density(U)
        self.V = self._to_flux_density(V)
        self.spix = self._to_decimal(spix)
        self.uvmin = self._to_decimal(uvmin)
        self.uvmax = self._to_decimal(uvmax)
        self.origin = origin
        self.age = age
        self.queried_at = queried_at

    def _to_flux_density(self, arg):
        """
        Return arg as a new FluxDensity. If arg is a number, it is assumed to
        be the flux density in Jy.
        """
        if isinstance(arg, FluxDensity):
            # create defensive copies of the flux arguments so they're not
            # shared between instances
            return FluxDensity(arg.value, arg.units)

        try:
            return FluxDensity(arg, FluxDensityUnits.JANSKY)
        except:
            raise ValueError('Could not convert {!r} to FluxDensity'.format(arg))

    def _to_decimal(self, arg):
        if isinstance(arg, Decimal):
            return arg
        elif isinstance(arg, (int, float)):
            return Decimal(list(map(str, arg)))
        else:
            raise ValueError('Could not convert {!r} to Decimal'.format(arg))

    @property
    def casa_flux_density(self):
        iquv = [self.I.to_units(FluxDensityUnits.JANSKY),
                self.Q.to_units(FluxDensityUnits.JANSKY),
                self.U.to_units(FluxDensityUnits.JANSKY),
                self.V.to_units(FluxDensityUnits.JANSKY)]
        return list(map(float, iquv))

    def __str__(self):
        return '<FluxMeasurement(Spw #{spw}, IQUV=({iquv}), spix={spix}, uvmin={uvmin}, uvmax={uvmax}, origin={origin}>'.format(
            spw=self.spw_id,
            iquv=','.join(map(str, (self.I, self.Q, self.U, self.V))),
            spix=float(self.spix),
            uvmin=float(self.uvmin),
            uvmax=float(self.uvmax),
            origin=self.origin
        )

    def __add__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError("unsupported operand type(s) for +: '%s' and '%s'" % (self.__class__.__name__,
                                                                                  other.__class__.__name__))

        spw_id = self.spw_id
        I = self.I + other.I
        Q = self.Q + other.Q
        U = self.U + other.U
        V = self.V + other.V
        spix = self.spix
        uvmin = self.uvmin
        uvmax = self.uvmax

        return self.__class__(spw_id, I, Q, U, V, spix, uvmin, uvmax)

    def __truediv__(self, other):
        if not isinstance(other, (int, float, Decimal)):
            raise TypeError("unsupported operand type(s) for /: '%s' and '%s'" % (self.__class__.__name__,
                                                                                  other.__class__.__name__))

        spw_id = self.spw_id
        I = self.I / other
        Q = self.Q / other
        U = self.U / other
        V = self.V / other
        spix = self.spix
        uvmin = self.uvmin
        uvmax = self.uvmax

        return self.__class__(spw_id, I, Q, U, V, spix, uvmin, uvmax)

    def __truediv__(self, other):
        return self.__div__(other)

    def __mul__(self, other):
        if not isinstance(other, (int, float, Decimal)):
            raise TypeError("unsupported operand type(s) for *: '%s' and '%s'" % (self.__class__.__name__,
                                                                                  other.__class__.__name__))

        spw_id = self.spw_id
        I = self.I * other
        Q = self.Q * other
        U = self.U * other
        V = self.V * other
        spix = self.spix
        uvmin = self.uvmin
        uvmax = self.uvmax

        return self.__class__(spw_id, I, Q, U, V, spix, uvmin, uvmax,)

    def __rmul__(self, other):
        if not isinstance(other, (int, float, Decimal)):
            raise TypeError("unsupported operand type(s) for *: '%s' and '%s'" % (self.__class__.__name__,
                                                                                  other.__class__.__name__))

        spw_id = self.spw_id
        I = self.I * other
        Q = self.Q * other
        U = self.U * other
        V = self.V * other
        spix = self.spix
        uvmin = self.uvmin
        uvmax = self.uvmax

        return self.__class__(spw_id, I, Q, U, V, spix, uvmin, uvmax)
