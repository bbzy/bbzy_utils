from unittest.case import TestCase
from unittest import main


class UnitTestCase(TestCase):
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
        from bbzy_utils.collection_utils import dict_get, iter_adj, dict_set_default, find, rfind, list_select_context, \
            list_remove_context, dict_select_context, dict_remove_context


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

    def test_serializatin(self):
        pass

    def test_singleton(self):
        pass


if __name__ == '__main__':
    main()
