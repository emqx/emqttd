-define(VALID, emqx_resource_validator).
-define(NOT_EMPTY(MSG), ?VALID:not_empty(MSG)).
-define(MAX(MAXV), ?VALID:max(number, MAXV)).
-define(MIN(MINV), ?VALID:min(number, MINV)).
