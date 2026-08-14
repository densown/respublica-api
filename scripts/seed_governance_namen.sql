-- Deutsche und englische Namen der Governance-Dimensionen. Idempotent.
--
--   mysql respublica_gesetze < scripts/seed_governance_namen.sql
--
-- In data_indicators stand in name_de durchgaengig der englische Text mit
-- dem Zusatz "(estimate)" — die Spalte war fuer diese sechs Indikatoren nie
-- gefuellt worden. Der Zusatz ist ausserdem Rauschen: dass ein Index eine
-- Schaetzung ist, gehoert in die Beschreibung, nicht in den Namen.
--
-- Die Uebersetzungen sind redaktionell und bewusst kurz gehalten, weil sie
-- als Achsen- und Spaltenbeschriftung verwendet werden.

UPDATE data_indicators SET
  name_de = 'Mitsprache und Rechenschaft',
  name_en = 'Voice and accountability',
  description_de = 'Inwieweit Bürgerinnen und Bürger an der Regierungsbildung mitwirken können, dazu Meinungs-, Vereinigungs- und Pressefreiheit.',
  description_en = 'The extent to which citizens can participate in selecting their government, plus freedom of expression, association and the press.'
WHERE code = 'VA.EST';

UPDATE data_indicators SET
  name_de = 'Rechtsstaatlichkeit',
  name_en = 'Rule of law',
  description_de = 'Vertrauen in die Regeln der Gesellschaft und deren Einhaltung — Vertragstreue, Eigentumsrechte, Polizei und Gerichte.',
  description_en = 'Confidence in and adherence to the rules of society — contract enforcement, property rights, police and courts.'
WHERE code = 'RL.EST';

UPDATE data_indicators SET
  name_de = 'Korruptionskontrolle',
  name_en = 'Control of corruption',
  description_de = 'Inwieweit öffentliche Ämter für privaten Vorteil genutzt werden, von der Alltagskorruption bis zur Vereinnahmung des Staates durch Eliten.',
  description_en = 'The extent to which public power is exercised for private gain, from petty corruption to state capture by elites.'
WHERE code = 'CC.EST';

UPDATE data_indicators SET
  name_de = 'Politische Stabilität',
  name_en = 'Political stability',
  description_de = 'Wahrscheinlichkeit politisch motivierter Gewalt oder eines verfassungswidrigen Regierungswechsels.',
  description_en = 'The likelihood of politically motivated violence or unconstitutional change of government.'
WHERE code = 'PV.EST';

UPDATE data_indicators SET
  name_de = 'Regierungseffektivität',
  name_en = 'Government effectiveness',
  description_de = 'Qualität öffentlicher Leistungen und der Verwaltung, ihre Unabhängigkeit von politischem Druck und die Verlässlichkeit der Umsetzung.',
  description_en = 'Quality of public services and the civil service, its independence from political pressure, and credibility of implementation.'
WHERE code = 'GE.EST';

UPDATE data_indicators SET
  name_de = 'Regulierungsqualität',
  name_en = 'Regulatory quality',
  description_de = 'Fähigkeit des Staates, Regeln zu setzen, die private Wirtschaftstätigkeit ermöglichen statt sie zu behindern.',
  description_en = 'The ability of government to formulate rules that permit and promote private sector development.'
WHERE code = 'RQ.EST';
