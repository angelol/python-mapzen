import json
from urllib import quote
from urllib2 import Request, urlopen, HTTPError

from mapzen.exceptions import MapzenError, MapzenRateLimitError, MapzenKeyError

__author__ = 'duydo'


def check_not_empty(reference, message=None):
    if reference:
        return reference
    raise ValueError(message)


class MapzenAPI(object):
    """Python Client for Mapzen APIs"""
    DEFAULT_VERSION = 'v1'
    

    def __init__(self, route_host=None, search_host=None, libpostal_host=None, api_key=None, version=None):
        if search_host is not None:
            self.SEARCH_BASE_URL = search_host
        else:
            self.SEARCH_BASE_URL = 'https://search.mapzen.com'
            
        self.api_key = api_key
        self.version = version or self.DEFAULT_VERSION
        self.BASE_PARAMS = ('sources', 'layers', 'boundary_country')

        # Autocomplete endpoint
        self.AUTOCOMPLETE_API_BASEURL = self.SEARCH_BASE_URL
        self.AUTOCOMPLETE_API_ENDPOINT = 'autocomplete'
        self.AUTOCOMPLETE_API_PARAMS = self.BASE_PARAMS + ('text', 'focus_point_lat', 'focus_point_lon')

        # Search endpoint
        self.SEARCH_API_BASEURL = self.SEARCH_BASE_URL
        self.SEARCH_API_ENDPOINT = 'search'
        self.SEARCH_API_PARAMS = self.AUTOCOMPLETE_API_PARAMS + (
            'size', 'boundary_rect_min_lat', 'boundary_rect_min_lon', 'boundary_rect_max_lat', 'boundary_rect_max_lon',
            'boundary_circle_lat', 'boundary_circle_lon', 'boundary_circle_radius'
        )

        # Reverse endpoint
        self.REVERSE_API_BASEURL = self.SEARCH_BASE_URL
        self.REVERSE_API_ENDPOINT = 'reverse'
        self.REVERSE_API_PARAMS = self.BASE_PARAMS + ('size', 'point_lat', 'point_lon')

        #Libpostal (beta) is handled on a different base URL and has no versioning
        if libpostal_host is not None:
            self.LIBPOSTAL_BASE_URL = libpostal_host
        else:
            self.LIBPOSTAL_BASE_URL = 'https://libpostal.mapzen.com'
            
        if route_host is not None:
            self.ROUTE_API_BASE_URL = route_host
        else:
            self.ROUTE_API_BASE_URL = 'https://valhalla.mapzen.com'
        self.ROUTE_API_ENDPOINT = 'route'
        self.ROUTE_API_PARAMS = ('json', )
        # Parse address endpoint (libpostal)
        self.PARSE_API_BASEURL = self.LIBPOSTAL_BASE_URL
        self.PARSE_API_ENDPOINT = 'parse'
        self.PARSE_API_PARAMS = ('address', 'format')

        # Expand address endpoint (libpostal)
        self.EXPAND_API_BASEURL = self.LIBPOSTAL_BASE_URL
        self.EXPAND_API_ENDPOINT = 'expand'
        self.EXPAND_API_PARAMS = ('address')

        # Use X-Cache to improve performance
        # Mapzen use CDN to help reduce this effect and limit the impact of common queries on its application servers.
        # See https://mapzen.com/documentation/search/api-keys-rate-limits/#caching-to-improve-performance
        self.x_cache = 'HIT'

    def search(self, text, **kwargs):
        """
        Geospatial search. Finding a specific location's geographic coordinates.
        The search endpoint document: https://mapzen.com/documentation/search/search/
        Args:
            text: A location to search
            **kwargs: Optional parameters
                size: Number for results are returned. Default 10
                boundary_country: An alpha-2 or alpha-3 ISO country code
                boundary_rect_min_lat: The minimum latitude for searching within a rectangular region
                boundary_rect_min_lon: The minimum longitude for searching within a rectangular region
                boundary_rect_max_lat: The maximum latitude for searching within a rectangular region
                boundary_rect_max_lon: The maximum longitude for searching within a rectangular region
                boundary_circle_lat: The latitude for searching within a circular region
                boundary_circle_lon: The longitude for searching within a circular region
                boundary_circle_radius: The acceptable distance (kilometers) for searching within a circular region
                focus_point_lat: The latitude for places with higher scores will appear higher in the results list
                focus_point_lon: The longitude for places with higher scores will appear higher in the results list
                sources: A comma-delimited string array, such as: openstreetmap, openaddresses, whosonfirst, geonames
                layers: A comma-delimited string array, such as: venue, address, street, country, macroregion, region, macrocounty, county, locality, localadmin, borough, neighbourhood, coarse
        Returns:
            GeoJSON
        Raises:
            ValueError if input param value is invalid
            MapzenRateLimitError if rate limit exceeded
            MapzenError if any error occurs, excepts above errors
        """
        kwargs['text'] = check_not_empty(text)
        return self._make_request(
            self._prepare_endpoint(self.SEARCH_API_BASEURL, self.SEARCH_API_ENDPOINT),
            self._prepare_params(kwargs, self.SEARCH_API_PARAMS)
        )
        
    def route(self, a, b, **kwargs):
        data = {'locations': []}
        for point in (a, b):            
            data['locations'].append({
                "lat": point[0],
                "lon": point[1],
                "type": "break",
            })
        data['costing'] = 'auto_shorter'
        kwargs['json'] = json.dumps(data)
        print 'kwargs: ', kwargs
        return self._make_request(
            self._prepare_endpoint_noversion(self.ROUTE_API_BASE_URL, self.ROUTE_API_ENDPOINT),
            kwargs,
        )

    def reverse(self, point_lat, point_lon, **kwargs):
        """
        Reverse geocoding. Finding places or addresses near a specific latitude, longitude pair.
        The reverse geocoding document: https://mapzen.com/documentation/search/reverse/
        Args:
            point_lat: The latitude to be reversed
            point_lon: The longitude to be reversed
            **kwargs: Optional parameters
                size: Number for results will be returned.
                boundary_country: An alpha-2 or alpha-3 ISO country code
                sources: A comma-delimited string array, such as: openstreetmap, openaddresses, whosonfirst, geonames
                layers: A comma-delimited string array, such as: venue, address, street, country, macroregion, region, macrocounty, county, locality, localadmin, borough, neighbourhood, coarse
        Returns:
            GeoJSON
        Raises:
            ValueError if input param value is invalid
            MapzenRateLimitError if rate limit exceeded
            MapzenError if any error occurs, excepts above errors
        """
        kwargs['point_lat'] = point_lat
        kwargs['point_lon'] = point_lon
        return self._make_request(
            self._prepare_endpoint(self.REVERSE_API_BASEURL, self.REVERSE_API_ENDPOINT),
            self._prepare_params(kwargs, self.REVERSE_API_PARAMS)
        )

    def autocomplete(self, text, **kwargs):
        """
        Search with autocomplete.
        The Autocomplete endpoint document https://mapzen.com/documentation/search/autocomplete/
        Args:
            text: A location to search
            **kwargs: Optional parameters
                size: Number for results are returned. Default 10
                boundary_country: An alpha-2 or alpha-3 ISO country code. Searching within a country
                boundary_rect_min_lat: The minimum latitude for searching within a rectangular region
                boundary_rect_min_lon: The minimum longitude for searching within a rectangular region
                boundary_rect_max_lat: The maximum latitude for searching within a rectangular region
                boundary_rect_max_lon: The maximum longitude for searching within a rectangular region
                boundary_circle_lat: The latitude for searching within a circular region
                boundary_circle_lon: The longitude for searching within a circular region
                boundary_circle_radius: The acceptable distance (kilometers) for searching within a circular region
                focus_point_lat: The latitude for places with higher scores will appear higher in the results list
                focus_point_lon: The longitude for places with higher scores will appear higher in the results list
                sources: A comma-delimited string array, such as: openstreetmap, openaddresses, whosonfirst, geonames
                layers: A comma-delimited string array, such as: venue, address, street, country, macroregion, region, macrocounty, county, locality, localadmin, borough, neighbourhood, coarse
        Returns:
            GeoJSON
        Raises:
            ValueError if input param value is invalid
            MapzenRateLimitError if rate limit exceeded
            MapzenError if any error occurs, excepts above errors
        """
        kwargs['text'] = check_not_empty(text)
        return self._make_request(
            self._prepare_endpoint(self.AUTOCOMPLETE_API_BASEURL, self.AUTOCOMPLETE_API_ENDPOINT),
            self._prepare_params(kwargs, self.AUTOCOMPLETE_API_PARAMS)
        )
        
    def parse(self, address, **kwargs):
        """
        Parse an address (work out what each block of text in the address is likely to represent/mean)
        Args:
            address: A text string representing an address
            **kwargs: Optional parameters
                 format: What format to return the results in. Default: List of dicationaries, containing label and value. Alternative is keys
        """
        kwargs['address'] = check_not_empty(address)
        return self._make_request(
            self._prepare_endpoint_noversion(self.PARSE_API_BASEURL, self.PARSE_API_ENDPOINT),
            self._prepare_params(kwargs, self.PARSE_API_PARAMS)
        )
        
    def expand(self, address, **kwargs):
        """
        Expand an address (normalise and remove abbreviations)
         Args:
            address: A text string representing an address
        """
        kwargs['address'] = check_not_empty(address)
        return self._make_request(
            self._prepare_endpoint_noversion(self.EXPAND_API_BASEURL, self.EXPAND_API_ENDPOINT),
            self._prepare_params(kwargs, self.EXPAND_API_PARAMS)
        )

    def use_api_key(self, api_key):
        """Use another api key"""
        self.api_key = check_not_empty(api_key)
        return self

    def use_hit_cache(self):
        """Use X-Cache: HIT for HTTP request headers"""
        self.x_cache = 'HIT'
        return self

    def use_miss_cache(self):
        """Use X-Cache: MISS for HTTP request headers"""
        self.x_cache = 'MISS'
        return self

    def _prepare_endpoint(self, endpoint_baseurl, endpoint_type):
        return '%s/%s/%s' % (endpoint_baseurl, self.version, endpoint_type)

    # Mapzen's libpostal API does not use versioning
    def _prepare_endpoint_noversion(self, endpoint_baseurl, endpoint_type):
        return '%s/%s' % (endpoint_baseurl, endpoint_type)

    def _prepare_params(self, params, allowed_params):
        _params = {'api_key': self.api_key}
        for k, v in params.iteritems():
            if k in allowed_params:
                _params[k.replace('_', '.')] = v.encode('utf-8') if isinstance(v, unicode) else v
        return _params

    def _prepare_request(self, endpoint, params):
        request = Request('%s?%s' % (endpoint, urlencode(params, quote_via=quote)))
        request.add_header('X-Cache', self.x_cache)
        return request

    def _make_request(self, endpoint, params):
        try:
            request = self._prepare_request(endpoint, params)
            response = urlopen(request)
            return json.loads(response.read(), encoding='utf-8')
        except HTTPError as e:
            self._raise_exceptions_for_status(e)
        except Exception as e:
            raise
            raise MapzenError(str(e))

    @staticmethod
    def _raise_exceptions_for_status(e):
        status_code = e.getcode()
        reason = None
        if 400 <= status_code < 500:
            if status_code == 403:
                reason = '%s Forbidden: %s for url: %s' % (status_code, e.reason, e.geturl())
                raise MapzenKeyError(reason, status_code=status_code)
            elif status_code == 429:
                reason = '%s Too Many Requests: %s for url: %s' % (status_code, e.reason, e.geturl())
                raise MapzenRateLimitError(reason, status_code=status_code)
            else:
                reason = '%s Client Error: %s for url: %s' % (status_code, e.reason, e.geturl())
        elif 500 <= status_code < 600:
            reason = '%s Server Error: %s for url: %s' % (status_code, e.reason, e.geturl())
        if reason:
            raise MapzenError(reason, status_code=status_code)

from urllib import quote_plus
def urlencode(query, doseq=0, quote_via=quote_plus):
    """Encode a sequence of two-element tuples or dictionary into a URL query string.
    If any values in the query arg are sequences and doseq is true, each
    sequence element is converted to a separate parameter.
    If the query arg is a sequence of two-element tuples, the order of the
    parameters in the output will match the order of parameters in the
    input.
    """

    if hasattr(query,"items"):
        # mapping objects
        query = query.items()
    else:
        # it's a bother at times that strings and string-like objects are
        # sequences...
        try:
            # non-sequence items should not work with len()
            # non-empty strings will fail this
            if len(query) and not isinstance(query[0], tuple):
                raise TypeError
            # zero-length sequences of all types will get here and succeed,
            # but that's a minor nit - since the original implementation
            # allowed empty dicts that type of behavior probably should be
            # preserved for consistency
        except TypeError:
            ty,va,tb = sys.exc_info()
            raise TypeError, "not a valid non-string sequence or mapping object", tb

    l = []
    if not doseq:
        # preserve old behavior
        for k, v in query:
            k = quote_via(str(k))
            v = quote_via(str(v))
            l.append(k + '=' + v)
    else:
        for k, v in query:
            k = quote_via(str(k))
            if isinstance(v, str):
                v = quote_via(v)
                l.append(k + '=' + v)
            elif _is_unicode(v):
                # is there a reasonable way to convert to ASCII?
                # encode generates a string, but "replace" or "ignore"
                # lose information and "strict" can raise UnicodeError
                v = quote_via(v.encode("ASCII","replace"))
                l.append(k + '=' + v)
            else:
                try:
                    # is this a sufficient test for sequence-ness?
                    len(v)
                except TypeError:
                    # not a sequence
                    v = quote_via(str(v))
                    l.append(k + '=' + v)
                else:
                    # loop over the sequence
                    for elt in v:
                        l.append(k + '=' + quote_via(str(elt)))
    return '&'.join(l)    