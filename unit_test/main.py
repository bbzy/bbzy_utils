import os
import shutil
import datetime
from typing import Dict, List, Tuple
from bbzy_utils.serializing import SerializableJsonObject, load_json
from unittest.case import TestCase
from unittest import main


class TestSerializingClass:
    def __init__(self, int_arg: int):
        self.int_arg = int_arg

    def to_jsonable(self):
        return {"int_arg": self.int_arg}

    @classmethod
    def from_jsonable(cls, jsonable_object):
        new_object = object.__new__(cls)
        new_object.int_arg = jsonable_object['int_arg']
        return new_object


class UnitTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        from bbzy_utils.path import DelayDirPath
        super().__init__(*args, **kwargs)
        shutil.rmtree('__test_workspace__', True)
        self.workspace_dir = DelayDirPath('__test_workspace__')

    def test_best_keeper(self):
        from bbzy_utils.best_keeper import BestKeeper
        test_list = [
            (1, True),
            (6, True),
            (3, False),
            (7, True),
            (-1, False),
        ]
        bk = BestKeeper()
        for i, replaced in test_list:
            self.assertEqual(bk.set_into(i), replaced)
        self.assertEqual(bk.score, 7)
        self.assertEqual(bk.addition, None)
        bk.reset()
        self.assertEqual(bk.score, None)
        self.assertEqual(bk.addition, None)
        test_list = [
            (1, '44'),
            (32, '33'),
            (2, '55'),
        ]
        for i, addition in test_list:
            bk.set_into(i, addition)
        self.assertEqual(bk.score, 32)
        self.assertEqual(bk.addition, '33')

    def test_call_once(self):
        from bbzy_utils.call_once import call_once

        @call_once
        def foo(i_, a_: list):
            a_.append(i_)
            return i_

        a = list()
        self.assertEqual(foo(1, a), 1)
        self.assertListEqual(a, [1])
        self.assertEqual(foo(1, a), None)
        self.assertEqual(foo(1, a), None)
        self.assertListEqual(a, [1])

    def test_collection_utils(self):
        pass

    def test_collections(self):
        pass

    def test_cooldown(self):
        pass

    def test_logging(self):
        pass

    def test_multiprocessing(self):
        pass

    def test_path(self):
        pass

    def test_serializing(self):
        self._test_serializable_json_object()

    def test_singleton(self):
        pass

    def _test_serializable_json_object(self):
        json_object_base_path = self.workspace_dir('serializable_json_object')
        json_object = SerializableJsonObject(
            json_object_base_path,
            Dict[datetime.date, List[Tuple[int, TestSerializingClass]]],
            dict(),
            {datetime.date: lambda _dt: datetime.datetime.strptime(_dt, '%Y-%m-%d').date()},
            {datetime.date: lambda _dt: datetime.date.strftime(_dt, '%Y-%m-%d')}
        )  # type: SerializableJsonObject[Dict[datetime.date, List[Tuple[int, TestSerializingClass]]]]
        self.assertFalse(os.path.exists(json_object.path))
        json_object.set_object({
            datetime.date(1991, 7, 19): [(1, TestSerializingClass(3))],
            datetime.date(2020, 7, 19): [(2, TestSerializingClass(28))],
        })
        self.assertTrue(os.path.isfile(json_object.path))
        json_object_file_dict = load_json(json_object_base_path)
        with json_object as json_object_context:
            json_object_context.r()[datetime.date(2014, 1, 1)] = [(4, TestSerializingClass(3))]
            self.assertDictEqual(json_object_file_dict, load_json(json_object_base_path))
        self.assertDictEqual(json_object_file_dict, load_json(json_object_base_path))
        with json_object as json_object_context:
            json_object_context.w().pop(datetime.date(2020, 7, 19))
            self.assertDictEqual(json_object_file_dict, load_json(json_object_base_path))
        json_object_file_dict['2014-01-01'] = [[4, {'int_arg': 3}]]
        json_object_file_dict.pop('2020-07-19')
        self.assertDictEqual(json_object_file_dict, load_json(json_object_base_path))


if __name__ == '__main__':
    main()
