const test = require('node:test');
const assert = require('node:assert/strict');
const engine = require('../app/static/js/bet-generator.js');

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

test('processamento parcial nunca é completo', () => {
  assert.deepEqual(engine.analysisMetadata(100, 60, 0).analysis_complete, false);
  assert.deepEqual(engine.analysisMetadata(100, 100, 0).analysis_complete, true);
  assert.deepEqual(engine.analysisMetadata(100, 99, 1).analysis_complete, false);
});

test('linhas aninhadas são altamente correlacionadas', () => {
  const a = {fixtureId: 'x', marketGroup: 'goals_ft'};
  const b = {fixtureId: 'x', marketGroup: 'goals_ft'};
  assert.equal(engine.correlationPenalty(a, b), 100);
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

test('mercados do primeiro tempo formam categorias próprias no bilhete', () => {
  assert.equal(engine.marketCategory({marketGroup: 'corners_1h_home'}), 'corners_1h');
  assert.equal(engine.marketCategory({marketGroup: 'cards_1h_total'}), 'cards_1h');
});

test('linha do primeiro tempo não duplica a mesma linha do jogo completo para a equipe', () => {
  const full = {fixtureId: 'nested', marketGroup: 'corners_away'};
  const firstHalf = {fixtureId: 'nested', marketGroup: 'corners_1h_away'};
  assert.equal(engine.correlationPenalty(full, firstHalf), 100);
});

test('linha liberada somente para edição nunca volta para a geração automática', () => {
  const manual = {fixtureId: 'manual', marketGroup: 'corners_home', status: 'REJECTED',
    rejectionReasons: [engine.REJECTION.MANUAL_ONLY], confidenceScore: 95, rawProbability: 100,
    adjustedProbability: 97, dataQualityScore: 90, sourceStats: {home: {samples: 6, raw: 100}}};
  engine.diversifyApprovedPool([manual], ['corners']);
  assert.equal(manual.status, 'REJECTED');
});
