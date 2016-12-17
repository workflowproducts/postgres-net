
### net.text_to_uri(text)

Takes text as input, returns uri encoded text

Note that uri encoding a space character converts the space to three characters (%20).

SELECT net.text_to_uri('test 3');
--test%203


### net.uri_to_text(text)

Takes uri encoded text as input, returns uri decoded (normal) text

Note that uri encoding a space character converts the space to three characters (%20).

SELECT net.uri_to_text('test%203');
--test 3


### net.getpar(text,text)

Takes query string for first input, second input is the key for the value you want to extract

This function returns the value after it has been decoded using uri_to_text(text)

Note that uri encoding a space character converts the space to three characters (%20).

SELECT net.getpar('test1=value1&test2=value%202', 'test1');
--value1

SELECT net.getpar('test1=value1&test2=value%202', 'test2');
--value 2


### net.getpare(text,text)

Takes query string for first input, second input is the key for the value you want to extract

This function returns the value without decoding using uri_to_text(text)

Note that uri encoding a space character converts the space to three characters (%20).

SELECT net.getpare('test1=value1&test2=value%202', 'test1');
--value1

SELECT net.getpare('test1=value1&test2=value%202', 'test2');
--value%202


### net.jsonify(anyelement)

Takes string, date, integer, etc and returns a properly encoded and escaped JSON value.

SELECT net.jsonify('test'::text);
--"test"

SELECT net.jsonify(1);
--1
