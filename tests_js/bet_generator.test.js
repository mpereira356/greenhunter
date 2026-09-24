const test = require('node:test');
const assert = require('node:assert/strict');
const engine = require('../app/static/js/bet-generator.js');

test('baseline permanece formalmente identificado como legacy_v1', () => {
  assert.equal(engine.MODEL_VERSION, 'legacy_v1');
});

const series = (hits, total, hitValue = 2, missValue = 0) => [
  ...Array(hits).fill(hitValue), ...Array(Math.max(0, total - hits)).fill(missValue)
];

const goal = (fixtureId, h2h, home, away, line = 1.5) => engine.evaluateCandidate({
  fixtureId, marketType: line === 1.5 ? 'over15' : 'over25', marketGroup: 'goals_ft', scope: 'total', line,
  bases: {h2h, home, away}, supportingScore: 85
}, 'balanced');

test('bases 100/83/83 geram confiança alta', () => {
  const candidate = goal('a', series(6, 6), series(5, 6), series(5, 6));
  assert.equal(candidate.status, 'APPROVED');
  assert.ok(candidate.confidenceScore >= 80);
});

test('mercado menos usa valores abaixo da linha e preserva a direção', () => {
  const candidate = engine.evaluateCandidate({
    fixtureId: 'under', marketType: 'under25', marketGroup: 'goals_ft', direction: 'under', scope: 'total', line: 2.5,
    bases: {h2h: [0, 1, 2, 1, 2, 0], home: [1, 2, 0, 1, 2, 1], away: [0, 1, 2, 2, 1, 0]}, supportingScore: 90
  }, 'balanced');
  assert.equal(candidate.direction, 'under');
  assert.equal(candidate.rawProbability, 100);
  assert.equal(candidate.status, 'APPROVED');
});

test('prioridade H2H ignora a forma geral no cálculo de gols', () => {
  const candidate = engine.evaluateCandidate({
    fixtureId: 'h2h-first', marketType: 'over15', marketGroup: 'goals_ft', scope: 'total', sourcePriority: 'h2h', line: 1.5,
    bases: {h2h: [2, 3, 4, 2, 3, 2], home: [0, 0, 0, 0, 0, 0], away: [0, 0, 0, 0, 0, 0]}, supportingScore: 90
  }, 'balanced');
  assert.equal(candidate.rawProbability, 100);
  assert.equal(candidate.sourcePriority, 'h2h');
  assert.equal(candidate.status, 'APPROVED');
});

test('base 100/100/33 sofre penalização forte', () => {
  const candidate = goal('b', series(6, 6), series(6, 6), series(2, 6));
  assert.equal(candidate.status, 'REJECTED');
  assert.ok(candidate.rejectionReasons.includes(engine.REJECTION.WEAK_AWAY_BASE));
  assert.ok(candidate.rejectionReasons.includes(engine.REJECTION.HIGH_DIVERGENCE));
});

test('9/10 possui sustentação superior a 3/3', () => {
  const short = engine.evaluateCandidate({fixtureId: 's', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line: 2.5, bases: {home: series(3, 3, 4)}}, 'balanced');
  const long = engine.evaluateCandidate({fixtureId: 'l', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line: 2.5, bases: {home: series(9, 10, 4)}}, 'balanced');
  assert.ok(long.sampleScore > short.sampleScore);
  assert.ok(long.confidenceScore >= short.confidenceScore);
});

test('100% em amostra curta é calibrado e não tratado como certeza', () => {
  const short = engine.evaluateCandidate({fixtureId: 'short-perfect', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line: 2.5,
    bases: {home: series(3, 3, 4)}, supportingScore: 80, contextScore: 80}, 'balanced');
  const long = engine.evaluateCandidate({fixtureId: 'long-perfect', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line: 2.5,
    bases: {home: series(10, 10, 4)}, supportingScore: 80, contextScore: 80}, 'balanced');
  assert.ok(short.adjustedProbability < 90);
  assert.ok(long.adjustedProbability > short.adjustedProbability);
});

test('linha equilibrada não escolhe automaticamente a maior linha de 67%', () => {
  const values = [7, 5, 5, 4, 4, 3, 3, 3, 3, 2];
  const candidates = [2.5, 3.5, 4.5, 5.5].map((line) => engine.evaluateCandidate({fixtureId: 'c', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line, minimumLine: 2.5, bases: {home: values}, supportingScore: 88}, 'balanced'));
  const chosen = engine.chooseOptimalLine(candidates, 'balanced');
  assert.notEqual(chosen.line, 5.5);
  assert.ok(chosen.confidenceScore >= candidates.at(-1).confidenceScore);
});

test('ranking não força diversidade de categoria', () => {
  const items = [
    {fixtureId: 'a', confidenceScore: 92, adjustedProbability: 90, dataQualityScore: 90, consistencyScore: 90, valueScore: 50},
    {fixtureId: 'b', confidenceScore: 90, adjustedProbability: 88, dataQualityScore: 90, consistencyScore: 90, valueScore: 50},
    {fixtureId: 'c', confidenceScore: 77, adjustedProbability: 80, dataQualityScore: 90, consistencyScore: 90, valueScore: 50}
  ];
  assert.deepEqual(engine.rankCandidates(items).map((item) => item.fixtureId), ['a', 'b', 'c']);
});

test('ordem de carregamento não altera ranking global', () => {
  const a = {fixtureId: 'a', status: 'APPROVED', marketGroup: 'x', confidenceScore: 88, adjustedProbability: 88, dataQualityScore: 90, consistencyScore: 90, valueScore: 50};
  const b = {fixtureId: 'b', status: 'APPROVED', marketGroup: 'x', confidenceScore: 94, adjustedProbability: 92, dataQualityScore: 90, consistencyScore: 90, valueScore: 50};
  assert.equal(engine.buildTicket([a, b], 1)[0].fixtureId, 'b');
});

test('completa a quantidade pedida mesmo quando os jogos restantes repetem categoria', () => {
  const candidates = Array.from({length: 8}, (_, index) => ({
    fixtureId: `early-${index}`, status: 'APPROVED', marketGroup: index === 0 ? 'cards_total' : 'goals_ft',
    marketType: index === 0 ? 'cards_total' : 'over15', confidenceScore: 90 - index,
    adjustedProbability: 88, dataQualityScore: 90, consistencyScore: 90, valueScore: 50
  }));
  const ticket = engine.buildTicket(candidates, 6, 'balanced', 1);
  assert.equal(new Set(ticket.map((item) => item.fixtureId)).size, 6);
});

test('processamento parcial nunca é completo', () => {
  assert.deepEqual(engine.analysisMetadata(100, 60, 0).analysis_complete, false);
  assert.deepEqual(engine.analysisMetadata(100, 100, 0).analysis_complete, true);
  assert.deepEqual(engine.analysisMetadata(100, 99, 1).analysis_complete, false);
});

test('cobertura parcial ampla permite gerar apenas com jogos completos', () => {
  assert.equal(engine.partialAnalysisSufficient(525, 158, 3), true);
  assert.equal(engine.partialAnalysisSufficient(525, 40, 3), false);
  assert.equal(engine.partialAnalysisSufficient(20, 12, 3), true);
  assert.equal(engine.partialAnalysisSufficient(20, 9, 2), false);
});

test('linhas aninhadas são altamente correlacionadas', () => {
  const a = {fixtureId: 'x', marketGroup: 'goals_ft'};
  const b = {fixtureId: 'x', marketGroup: 'goals_ft'};
  assert.equal(engine.correlationPenalty(a, b), 100);
});

test('gol de uma equipe pertence a gols e não duplica total de gols no mesmo jogo', () => {
  const teamGoal = {fixtureId: 'tg', marketGroup: 'team_goals_home'};
  const totalGoal = {fixtureId: 'tg', marketGroup: 'goals_ft'};
  assert.equal(engine.marketCategory(teamGoal), 'goals');
  assert.equal(engine.correlationPenalty(teamGoal, totalGoal), 100);
});

test('H2H de um jogo recebe credibilidade pequena', () => {
  assert.equal(engine.h2hCredibility(1), .15);
  assert.equal(engine.h2hCredibility(6), 1);
});

test('dados completos superam dados incompletos em qualidade', () => {
  const complete = goal('full', series(6, 6), series(5, 6), series(5, 6));
  const incomplete = engine.evaluateCandidate({fixtureId: 'partial', marketType: 'corners_home', marketGroup: 'corners_home', scope: 'home', line: 2.5, bases: {home: series(3, 3, 4)}}, 'balanced');
  assert.ok(complete.dataQualityScore > incomplete.dataQualityScore);
});

test('linhas comerciais de chutes ao gol não começam em patamares muito baixos', () => {
  assert.equal(engine.CONFIG.marketMinimums.shots_on_target_total, 7.5);
  assert.equal(engine.CONFIG.marketMinimums.shots_on_target_home, 3.5);
  assert.equal(engine.CONFIG.marketMinimums.shots_on_target_away, 3.5);
});

test('equilibrado permite múltipla forte com até três mercados independentes', () => {
  const candidate = (marketGroup, confidence) => ({
    fixtureId: 'multi', status: 'APPROVED', marketGroup, confidenceScore: confidence,
    adjustedProbability: confidence, dataQualityScore: 90, consistencyScore: 90, valueScore: 50
  });
  const ticket = engine.buildTicket([
    candidate('goals_ft', 92), candidate('corners_home', 90), candidate('cards_total', 88), candidate('goal_ht', 87)
  ], 1, 'balanced');
  assert.equal(ticket.length, 3);
  assert.deepEqual(new Set(ticket.map((item) => item.marketGroup)), new Set(['goals_ft', 'corners_home', 'cards_total']));
});

test('mercado independente aprovado pode complementar o mesmo jogo', () => {
  const base = {fixtureId: 'complement', status: 'APPROVED', adjustedProbability: 86, dataQualityScore: 90, consistencyScore: 85, valueScore: 50};
  const ticket = engine.buildTicket([
    {...base, marketGroup: 'corners_home', confidenceScore: 89},
    {...base, marketGroup: 'goals_ft', confidenceScore: 75},
    {...base, marketGroup: 'cards_total', confidenceScore: 74, status: 'REJECTED'}
  ], 1, 'balanced');
  assert.deepEqual(new Set(ticket.map((item) => item.marketGroup)), new Set(['corners_home', 'goals_ft']));
});

test('finalizações e chutes ao gol não entram juntos na mesma múltipla', () => {
  const base = {fixtureId: 'attempts', status: 'APPROVED', adjustedProbability: 90, dataQualityScore: 90, consistencyScore: 90, valueScore: 50};
  const shots = {...base, marketGroup: 'shots_total', confidenceScore: 92};
  const target = {...base, marketGroup: 'shots_on_target_total', confidenceScore: 91};
  assert.equal(engine.correlationPenalty(shots, target), 60);
  assert.equal(engine.buildTicket([shots, target], 1, 'balanced').length, 1);
});

test('uma categoria não domina todos os jogos quando existem alternativas aprovadas', () => {
  const pick = (fixtureId, marketGroup, confidenceScore) => ({
    fixtureId, marketGroup, confidenceScore, status: 'APPROVED', adjustedProbability: confidenceScore,
    dataQualityScore: 90, consistencyScore: 85, valueScore: 50
  });
  const ticket = engine.buildTicket([
    pick('c1', 'corners_home', 95), pick('c2', 'corners_away', 94),
    pick('c3', 'corners_home', 93), pick('c4', 'corners_away', 92),
    pick('g1', 'goals_ft', 86), pick('k1', 'cards_total', 84)
  ], 4, 'balanced');
  const primaryFixtures = new Set(ticket.map((item) => item.fixtureId));
  const primary = [...primaryFixtures].map((fixtureId) => ticket.find((item) => item.fixtureId === fixtureId));
  assert.equal(primary.filter((item) => engine.marketCategory(item) === 'corners').length, 2);
  assert.ok(primary.some((item) => engine.marketCategory(item) === 'goals'));
  assert.ok(primary.some((item) => engine.marketCategory(item) === 'cards'));
});

test('cada categoria aprovada recebe uma vaga quando a quantidade permite', () => {
  const pick = (fixtureId, marketGroup, confidenceScore) => ({
    fixtureId, marketGroup, confidenceScore, status: 'APPROVED', adjustedProbability: confidenceScore,
    dataQualityScore: 90, consistencyScore: 85, valueScore: 50
  });
  const ticket = engine.buildTicket([
    pick('c1', 'corners_home', 95), pick('c2', 'corners_away', 94), pick('c3', 'corners_total', 93),
    pick('g1', 'goals_ft', 82), pick('k1', 'cards_total', 80), pick('s1', 'shots_on_target_total', 79)
  ], 6, 'balanced');
  const categories = new Set(ticket.map(engine.marketCategory));
  assert.deepEqual(categories, new Set(['corners', 'goals', 'cards', 'shots_on_target']));
});

test('recupera uma opção forte de categoria ausente sem aceitar dados frágeis', () => {
  const strong = engine.evaluateCandidate({fixtureId: 'g1', marketType: 'over15', marketGroup: 'goals_ft', scope: 'total', line: 1.5,
    bases: {home: series(5, 4, 3), away: series(5, 4, 3)}, supportingScore: 70}, 'balanced');
  strong.status = 'REJECTED';
  strong.rejectionReasons = [engine.REJECTION.LOW_CONFIDENCE];
  strong.confidenceScore = 72;
  strong.rawProbability = 80;
  strong.adjustedProbability = 76;
  strong.dataQualityScore = 70;
  const weak = {...strong, fixtureId: 'g2', sourceStats: {home: {samples: 2, raw: 100}, away: {samples: 2, raw: 100}}};
  const diversified = engine.diversifyApprovedPool([strong, weak], ['goals']);
  assert.equal(diversified[0].status, 'APPROVED');
  assert.equal(diversified[0].diversityFallback, true);
  assert.equal(diversified[1].status, 'REJECTED');
});

test('complementos independentes consistentes entram mesmo abaixo do corte do mercado principal', () => {
  const primary = {fixtureId: 'multi-complementar', marketType: 'over15', marketGroup: 'goals_ft', scope: 'total',
    status: 'APPROVED', confidenceScore: 96, rawProbability: 100, adjustedProbability: 94,
    dataQualityScore: 90, consistencyScore: 90, valueScore: 50, sourceStats: {home: {samples: 6, raw: 100}, away: {samples: 6, raw: 100}}};
  const complement = {fixtureId: 'multi-complementar', marketType: 'cards_total', marketGroup: 'cards_total', scope: 'total',
    status: 'REJECTED', rejectionReasons: [engine.REJECTION.LOW_CONFIDENCE], strengths: [],
    confidenceScore: 67, rawProbability: 75, adjustedProbability: 68, dataQualityScore: 60,
    consistencyScore: 75, valueScore: 40, sourceStats: {home: {samples: 6, raw: 67}, away: {samples: 6, raw: 83}}};
  engine.diversifyApprovedPool([primary, complement], ['goals', 'cards']);
  const ticket = engine.buildTicket([primary, complement], 1, 'balanced', 6);
  assert.equal(complement.status, 'APPROVED');
  assert.equal(complement.diversityFallback, true);
  assert.deepEqual(new Set(ticket.map(engine.marketCategory)), new Set(['goals', 'cards']));
});

test('complemento continua bloqueado quando alguma base primária possui menos de três jogos', () => {
  const weak = {fixtureId: 'weak-complement', marketType: 'cards_total', marketGroup: 'cards_total', scope: 'total',
    status: 'REJECTED', rejectionReasons: [engine.REJECTION.LOW_CONFIDENCE], strengths: [],
    confidenceScore: 70, rawProbability: 80, adjustedProbability: 70, dataQualityScore: 60,
    sourceStats: {home: {samples: 2, raw: 100}, away: {samples: 6, raw: 83}}};
  engine.diversifyApprovedPool([weak], ['cards']);
  assert.equal(weak.status, 'REJECTED');
});

test('não duplica dois lados de escanteios e preserva mercado de outra categoria no jogo', () => {
  const pick = (marketGroup, confidenceScore) => ({fixtureId: 'mix', marketGroup, confidenceScore, status: 'APPROVED',
    adjustedProbability: confidenceScore, dataQualityScore: 90, consistencyScore: 85, valueScore: 50});
  const ticket = engine.buildTicket([
    pick('corners_home', 90), pick('corners_away', 88), pick('goals_ft', 78)
  ], 1, 'balanced');
  assert.deepEqual(ticket.map(engine.marketCategory).sort(), ['corners', 'goals']);
});

test('prioriza uma partida com múltiplos mercados fortes sobre uma opção isolada', () => {
  const pick = (fixtureId, marketGroup, confidenceScore) => ({fixtureId, marketGroup, confidenceScore, status: 'APPROVED',
    adjustedProbability: confidenceScore, dataQualityScore: 90, consistencyScore: 85, valueScore: 50});
  const ticket = engine.buildTicket([
    pick('isolado', 'corners_home', 95),
    pick('multipla', 'corners_home', 86), pick('multipla', 'goals_ft', 78), pick('multipla', 'cards_total', 76)
  ], 1, 'balanced');
  assert.equal(new Set(ticket.map((item) => item.fixtureId)).size, 1);
  assert.equal(ticket[0].fixtureId, 'multipla');
  assert.equal(ticket.length, 3);
});

test('mercados por tempo pertencem à categoria principal do esporte', () => {
  assert.equal(engine.marketCategory({marketGroup: 'corners_1h_home'}), 'corners');
  assert.equal(engine.marketCategory({marketGroup: 'cards_1h_total'}), 'cards');
  assert.equal(engine.marketCategory({marketGroup: 'cards_2h_total'}), 'cards');
});

test('linha do primeiro tempo não duplica a mesma linha do jogo completo para a equipe', () => {
  const full = {fixtureId: 'nested', marketGroup: 'corners_away'};
  const firstHalf = {fixtureId: 'nested', marketGroup: 'corners_1h_away'};
  assert.equal(engine.correlationPenalty(full, firstHalf), 100);
});

test('limite pessoal permite ampliar mercados independentes por partida', () => {
  const pick = (marketGroup, confidenceScore) => ({fixtureId: 'custom-limit', marketGroup, confidenceScore, status: 'APPROVED',
    adjustedProbability: confidenceScore, dataQualityScore: 90, consistencyScore: 85, valueScore: 50});
  const ticket = engine.buildTicket([
    pick('goals_ft', 90), pick('corners_home', 88), pick('cards_total', 86), pick('fouls_total', 84)
  ], 1, 'balanced', 4);
  assert.equal(ticket.length, 4);
});

test('linha liberada somente para edição nunca volta para a geração automática', () => {
  const manual = {fixtureId: 'manual', marketGroup: 'corners_home', status: 'REJECTED',
    rejectionReasons: [engine.REJECTION.MANUAL_ONLY], confidenceScore: 95, rawProbability: 100,
    adjustedProbability: 97, dataQualityScore: 90, sourceStats: {home: {samples: 6, raw: 100}}};
  engine.diversifyApprovedPool([manual], ['corners']);
  assert.equal(manual.status, 'REJECTED');
});

test('candidato razoável abaixo do corte principal permanece como alternativa', () => {
  const candidate = engine.evaluateCandidate({
    fixtureId: 'alternative', marketType: 'cards_total', marketGroup: 'cards_total', scope: 'total', line: 4.5,
    bases: {home: series(4, 6, 6), away: series(4, 6, 6)}, supportingScore: 65
  }, 'balanced');
  assert.equal(candidate.status, 'ALTERNATIVE');
  assert.ok(candidate.rejectionReasons.length > 0);
});

test('ausência de H2H não elimina mercado sustentado por casa e fora', () => {
  const candidate = engine.evaluateCandidate({
    fixtureId: 'no-h2h', marketType: 'over15', marketGroup: 'goals_ft', scope: 'total', line: 1.5,
    bases: {h2h: [], home: series(6, 6, 3), away: series(6, 6, 3)}, supportingScore: 90, contextScore: 85
  }, 'balanced');
  assert.equal(candidate.status, 'APPROVED');
  assert.equal(candidate.h2hSample, 0);
});

test('um jogo pode retornar até seis mercados independentes aprovados', () => {
  const pick = (marketGroup, confidenceScore) => ({fixtureId: 'six', marketGroup, confidenceScore, status: 'APPROVED',
    adjustedProbability: confidenceScore, dataQualityScore: 90, consistencyScore: 90, valueScore: 50});
  const candidates = [
    pick('goals_ft', 94), pick('corners_total', 92), pick('cards_total', 90),
    pick('fouls_total', 88), pick('offsides_total', 86), pick('shots_on_target_total', 84), pick('shots_total', 82)
  ];
  const ticket = engine.buildTicket(candidates, 1, 'balanced', 6);
  assert.equal(ticket.length, 6);
  assert.equal(new Set(ticket.map((candidate) => candidate.marketGroup)).size, 6);
});

test('funil de auditoria contabiliza aprovados alternativos rejeitados e deduplicados', () => {
  const candidates = [
    {fixtureId: 'audit', status: 'APPROVED', rejectionReasons: [], adjustedProbability: 80},
    {fixtureId: 'audit', status: 'ALTERNATIVE', rejectionReasons: [engine.REJECTION.LOW_CONFIDENCE], adjustedProbability: 70},
    {fixtureId: 'audit', status: 'REJECTED', rejectionReasons: [engine.REJECTION.LOW_SAMPLE], adjustedProbability: 90}
  ];
  const funnel = engine.auditCandidateFunnel(candidates, candidates.slice(0, 2));
  assert.deepEqual(funnel, {raw_candidates: 3, after_data_filters: 2, after_thresholds: 1,
    after_calibration: 1, after_deduplication: 2, approved: 1, alternatives: 1, rejected: 1});
});

test('debug de candidato mantém os metadados necessários para auditoria', () => {
  const candidate = goal('debug', series(6, 6), series(6, 6), series(5, 6));
  const debug = engine.candidateDebugRecord(candidate);
  for (const key of ['market', 'line', 'side', 'sample_size', 'historical_rate', 'probability',
    'calibrated_probability', 'confidence', 'strength', 'matchup', 'odds', 'final_score', 'classification', 'reasons']) {
    assert.ok(Object.hasOwn(debug, key), key);
  }
});
