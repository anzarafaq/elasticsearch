"""
Geocode and crack addresses with a third party service.
"""
from __future__ import unicode_literals

import base64
import hashlib
import hmac
import logging
import re
import json
import urllib2
import os

from werkzeug import url_encode
from urlparse import ParseResult, urlunparse

log = logging.getLogger(__name__)

# Thank you Shan
GOOGLE_MAPS_API_PREMIER_CLIENT_ID = "gme-ebayinc1"
GOOGLE_MAPS_API_PREMIER_CRYPTO_KEY = b"JZsjKLrAy0gXG2q_Bvq1yrz1keU="

ZIP_PLUS_FOUR_RE = re.compile(r'^(\d{5})-\d{4}$')


GOOGLE_MAPS_URL = ParseResult(scheme='http',
                              netloc='maps.googleapis.com',
                              path='/maps/api/geocode/json',
                              params='',
                              query='',
                              fragment='')


class GeocodeFailure(Exception):
    pass


def crack_address_return_partial(raw_address, allow_partial=True):
    """
    Same as crack_address, but we return an array of partial
    addresses if we don't get an exact match.
    """

    google_json = google_geocode(raw_address)
    if google_response_is_error(google_json):
        raise GeocodeFailure("Failed to geocode, geocoder status: %s"
                             % google_json["status"])

    return cracked_address_from_google_json_response(google_json,
                                                     raw_address,
                                                     allow_partial)


def crack_address(raw_address):
    """
    This is the main entry point for geocoding and address cracking.
    Parameters:
      raw_address: A freeform address as a string, for example
                   "165 University Ave., Palo Alto, CA"
    Returns:
      A CrackedAddress database model with as many instance variables
      as possible filled in.

    Raises:
      GeocodeFailure if there was error in the third-party service.
    """
    return crack_address_return_partial(raw_address, allow_partial=True)


def cracked_address_from_google_json_response(google_json,
                                              raw_address,
                                              allow_partial=False):
    results = []
    partial = []
    for result in google_json["results"]:
        if ("partial_match" not in result
            or result["partial_match"] != True):
            results.append(result)
        else:
            partial.append(result)
    if len(results) > 1:
        log.info("%d valid geocode results returned, choosing the first"
                    % len(results))
    if not len(results):
        if allow_partial:
            # TODO Do we need to worry about the
            # back_out_best_address_from_latlng logic for partial matches?
            return map(cracked_address_from_google_json, partial,
                       [raw_address] * len(partial))
        else:
            raise GeocodeFailure("Failed to geocode, no non-'partial_match'"
                                 " results returned")
    geocode_result = results[0]
    cracked_address = cracked_address_from_google_json(geocode_result,
                                                       raw_address)
    if cracked_address['postal_code']:
        # Google's returning 9-digit zips, which don't fit into the
        # current schema, so check for those and truncate them to
        # 5-digit.
        matches = ZIP_PLUS_FOUR_RE.search(cracked_address['postal_code'])
        if matches:
            cracked_address.postal_code = matches.group(1)
    else:
        raise GeocodeFailure('Google did not return a postal code')

    #CRACKED_ADDRESS_CACHE[raw_address] = cracked_address

    return cracked_address


def get_exact_address_from_latlng(latitude, longitude,
                                  allow_partial=False):
    """
    Given a lat/long pair, return the exact address.
    """
    raw_address = "%s,%s" % (latitude, longitude)
    return crack_address_return_partial(raw_address, allow_partial, False)


def cracked_address_from_google_json(google_json_result, raw_address):
    """
    Take a "result" subsection of a Google JSON response and
    marshal it into a CrackedAddress. More details on the response
    format here: http://code.google.com/apis/maps/documentation/geocoding/#JSON
    """
    cracked_address = {}
    street_address = {"number": None,
                      "street": None}

    for component in google_json_result["address_components"]:
        types = component["types"]
        if "street_number" in types:
            street_address["number"] = component["long_name"]
        if "route" in types:
            street_address["street"] = component["long_name"]
        if "locality" in types:
            cracked_address['locality_name'] = component["long_name"]
        if "administrative_area_level_1" in types:
            # State, 2-letter
            cracked_address['administrative_area_name'] \
                = component["short_name"]
        if "postal_code" in types:
            cracked_address['postal_code'] = component["long_name"]

    if not all(street_address.values()):
        cracked_address['thoroughfare_name'] = None
    else:
        cracked_address['thoroughfare_name'] = ("%(number)s %(street)s"
        % street_address)

    cracked_address['latitude'] = (google_json_result["geometry"]
                                ["location"]["lat"])
    cracked_address['longitude'] = (google_json_result["geometry"]
                                 ["location"]["lng"])
    cracked_address['raw_address'] = raw_address
    cracked_address['canonical_address'] = \
         google_json_result["formatted_address"]

    return cracked_address


def google_authorize_url(path, key=GOOGLE_MAPS_API_PREMIER_CRYPTO_KEY):
    """
    Returns parameters necessary for authenticating the Google Maps
    call.  parsed_url should be a urlparse.ParsedResult instance.
    """
    decoded_key = base64.urlsafe_b64decode(key)
    signature = hmac.new(decoded_key, path, hashlib.sha1)
    return dict(signature=base64.urlsafe_b64encode(signature.digest()))


def google_geocode(raw_address):
    """
    Get the JSON response from Google's geocoder.
    You should probably use crack_address unless you really
    want Google's JSON response.
    """
    #os.environ['http_proxy'] = "http://httpproxy.vip.ebay.com:80"
    #os.environ['https_proxy'] = "http://httpproxy.vip.ebay.com:80"

    params = dict(sensor='false',
                  address=raw_address,
                  client=GOOGLE_MAPS_API_PREMIER_CLIENT_ID)

    params.update(google_authorize_url(GOOGLE_MAPS_URL.path + '?'
                                       + url_encode(params, sort=True)))

    url = GOOGLE_MAPS_URL._replace(query=url_encode(params, sort=True))

    api_endpoint = urlunparse(url)
    response = unicode(urllib2.urlopen(api_endpoint).read(), "utf8")
    response_json = json.loads(response)

    return response_json


def google_response_is_error(google_json):
    return google_json["status"] != "OK"


def _normalise(location):
    #return location
    #if (location.longitude not in ('', None, 0.0) and
    #    location.latitude not in ('', None, 0.0)):
    #    return True, location

    address = ",".join([
                location.get('street'),
                location.get('city'),
                location.get('region'),
                location.get('postcode'),
                "USA"])
    #print address
    try:
        normalized_address = crack_address(address)
        #print normalized_address
    except Exception, ex:
        raise Exception("Unable to geocode address: %s" % address)

    if type(normalized_address) == type(list()):
        normalized_address = normalized_address[0]

    if (normalized_address['latitude'] in ('', None) or
        normalized_address['longitude'] in ('', None)):
        raise Exception("Unable to geocode address: %s" % address)

    return {
            "street": normalized_address['thoroughfare_name'],
            "city": normalized_address['locality_name'],
            "latitude": normalized_address['latitude'],
            "longitude": normalized_address['longitude'],
            "postcode": normalized_address['postal_code'],
            "state": normalized_address['administrative_area_name']}


if __name__ == '__main__':
    location = dict(street="4162 grey cliffs", city="san jose", region="california", postcode="95121")
    print _normalise(location)
