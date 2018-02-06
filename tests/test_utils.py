import unittest
from side8.k8s.operator import utils


class TestParse(unittest.TestCase):
    def test_parse_types(self):
        self.assertCountEqual(

            [("STR_KEY", "value"),
             ("INT_KEY", "1"),
             ("FLOAT_KEY", "2.5"),
             ("BOOL_TRUE_KEY", "1"),
             ("BOOL_FALSE_KEY", "0"),
             ("NONE_KEY", "")],

            utils.parse({
                "str_key": "value",
                "int_key": 1,
                "float_key": 2.5,
                "bool_true_key": True,
                "bool_false_key": False,
                "none_key": None}))

    def test_parse_nested(self):
        self.assertCountEqual(
            [("ONE_STR", "value1"),
             ("ONE_DICT_TWO_INT", "2"),
             ("ONE_DICT_TWO_LIST_0", "3"),
             ("ONE_DICT_TWO_LIST_1", "3"),
             ("ONE_DICT_TWO_LIST_2", "3.3")],

            utils.parse({
                "one_str": "value1",
                "one_dict": {
                    "two_int": 2,
                    "two_list": [
                        "3", 3, 3.3
                    ]
                }}))

    def test_parse_bad_type(self):
        with self.assertRaises(ValueError) as e:
            utils.parse({"bad": object()})
        self.assertEqual("type 'object' not supported", e.exception.args[0])
