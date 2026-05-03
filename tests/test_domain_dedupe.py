import unittest

from src.app.bonus import calcular_time
from src.app.domain import parse_sellers


class SellerDedupeTests(unittest.TestCase):
    def test_duplicate_rows_keep_projected_and_real_reach_separate(self) -> None:
        payload = {
            "vendedores": [
                {
                    "nome": "Ana Souza",
                    "alcance_projetado_pct": 92.0,
                    "alcance_pct": 64.0,
                    "margem_pct": 27.0,
                },
                {
                    "nome": "Ana Souza (1)",
                    "prazo_medio": 40,
                    "qtd_faturadas": 30,
                    "interacoes": 240,
                    "tme_minutos": 4.0,
                },
            ]
        }

        sellers = parse_sellers(payload)
        self.assertEqual(len(sellers), 1)
        self.assertEqual(sellers[0].alcance_projetado_pct, 92.0)
        self.assertEqual(sellers[0].alcance_pct, 64.0)

        results, _ = calcular_time(sellers)
        self.assertTrue(results[0].elegivel_margem)
        self.assertEqual(results[0].bonus_margem, 150.0)


if __name__ == "__main__":
    unittest.main()
