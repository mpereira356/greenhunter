(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  root.GreenHunterBetGenerator = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const CONFIG = Object.freeze({
    profiles: {
      conservative: {minConfidence: 85, minAdjusted: 82, minDataQuality: 75, secondMinConfidence: 101, secondMaxGap: 0, maxLineDrop: 4, maxSelectionsPerFixture: 1},
      balanced: {minConfidence: 75, preferredConfidence: 80, minAdjusted: 75, minDataQuality: 55, secondMinConfidence: 75, secondMaxGap: 100, maxLineDrop: 8, maxSelectionsPerFixture: 3},
      value: {minConfidence: 75, minAdjusted: 72, minDataQuality: 65, secondMinConfidence: 85, secondMaxGap: 7, maxLineDrop: 10, maxSelectionsPerFixture: 3}
    },
    weights: {adjustedProbability: .35, consistency: .20, sample: .15, recent: .15, supporting: .15},
    baseWeights: {
      total: {home: .40, away: .40, h2h: .20},
      home: {home: .85, h2h: .15},
      away: {away: .85, h2h: .15}
    },
    sampleConfidence: {0: 0, 1: 15, 2: 30, 3: 45, 4: 55, 5: 65, 6: 75, 7: 82, 8: 88, 9: 94, 10: 100},
    h2hCredibility: {0: 0, 1: .15, 2: .30, 3: .50, 4: .70, 5: .85, 6: 1},
    recencyWeights: [1, .95, .90, .85, .80, .75, .70, .65, .60, .55],
    marketMinimums: {
      corners_total: 8.5, corners_home: 2.5, corners_away: 2.5,
      cards_total: 4.5, cards_home: 1.5, cards_away: 1.5,
      shots_total: 19.5, shots_home: 10.5, shots_away: 10.5,
      shots_on_target_total: 7.5, shots_on_target_home: 3.5, shots_on_target_away: 3.5,
      fouls_total: 15.5, fouls_home: 7.5, fouls_away: 7.5,
      offsides_total: 1.5, offsides_home: .5, offsides_away: .5
    },
    lineSelection: {minimumHistoricalFrequency: 55, valueWeight: .08},
    ticketComposition: {maxPrimaryCategoryShare: .40},
    correlation: {goalsNested: 100, goalHtWithGoals: 65, totalWithTeamCorners: 65, attempts: 60, sameGroup: 100}
  });

  const REJECTION = Object.freeze({
    INSUFFICIENT_DATA: 'INSUFFICIENT_DATA', LOW_SAMPLE: 'LOW_SAMPLE', LOW_RAW_PROBABILITY: 'LOW_RAW_PROBABILITY',
    LOW_ADJUSTED_PROBABILITY: 'LOW_ADJUSTED_PROBABILITY', LOW_CONFIDENCE: 'LOW_CONFIDENCE', HIGH_DIVERGENCE: 'HIGH_DIVERGENCE',
    LOW_DATA_QUALITY: 'LOW_DATA_QUALITY', WEAK_HOME_BASE: 'WEAK_HOME_BASE', WEAK_AWAY_BASE: 'WEAK_AWAY_BASE',
    WEAK_SUPPORTING_METRICS: 'WEAK_SUPPORTING_METRICS', LINE_TOO_AGGRESSIVE: 'LINE_TOO_AGGRESSIVE',
    REDUNDANT_MARKET: 'REDUNDANT_MARKET', HIGH_CORRELATION: 'HIGH_CORRELATION', BETTER_LINE_AVAILABLE: 'BETTER_LINE_AVAILABLE',
    MANUAL_ONLY: 'MANUAL_ONLY'
  });

  const clamp = (value, min = 0, max = 100) => Math.max(min, Math.min(max, Number(value) || 0));
  const round1 = (value) => Math.round(Number(value) * 10) / 10;
  const mean = (values) => values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  const stddev = (values) => {
    const average = mean(values);
    return average === null ? 0 : Math.sqrt(mean(values.map((value) => (value - average) ** 2)));
  };

  function sampleScore(samples) {
    const n = Math.max(0, Math.min(10, Math.floor(Number(samples) || 0)));
    return CONFIG.sampleConfidence[n];
  }

  function h2hCredibility(samples) {
    const n = Math.max(0, Math.floor(Number(samples) || 0));
    return CONFIG.h2hCredibility[Math.min(6, n)];
  }

  function seriesStats(values, predicate) {
    const clean = (values || []).map(Number).filter(Number.isFinite).slice(0, 10);
    if (!clean.length) return null;
    const hits = clean.filter(predicate).length;
    const raw = hits / clean.length * 100;
    let weightedHits = 0;
    let weightTotal = 0;
    clean.forEach((value, index) => {
      const weight = CONFIG.recencyWeights[index] || CONFIG.recencyWeights[CONFIG.recencyWeights.length - 1];
      weightTotal += weight;
      if (predicate(value)) weightedHits += weight;
    });
    return {samples: clean.length, hits, raw, recent: weightTotal ? weightedHits / weightTotal * 100 : raw, average: mean(clean), values: clean};
  }

  function consistencyScore(probabilities) {
    const values = probabilities.map(Number).filter(Number.isFinite);
    if (!values.length) return 0;
    if (values.length === 1) return 70;
    const range = Math.max(...values) - Math.min(...values);
    const deviation = stddev(values);
    const rangeScore = range <= 10 ? 100 : range <= 20 ? 90 : range <= 30 ? 75 : range <= 40 ? 55 : range <= 50 ? 35 : 10;
    return round1(clamp(rangeScore - Math.max(0, deviation - 8) * .6));
  }

  function dataQualityScore(stats, scope, supportingAvailable) {
    const primaryKeys = scope === 'home' ? ['home'] : scope === 'away' ? ['away'] : ['home', 'away'];
    const presentPrimary = primaryKeys.filter((key) => stats[key] && stats[key].samples >= 3).length;
    const primaryCoverage = primaryKeys.length ? presentPrimary / primaryKeys.length : 0;
    const h2h = stats.h2h;
    const all = Object.values(stats).filter(Boolean);
    const sampleComponent = all.length ? mean(all.map((item) => sampleScore(item.samples))) : 0;
    const baseComponent = primaryCoverage * 65 + (h2h ? Math.min(15, h2hCredibility(h2h.samples) * 15) : 0);
    const supportComponent = supportingAvailable ? 20 : 8;
    return round1(clamp(baseComponent + sampleComponent * .15 + supportComponent));
  }

  function qualityGrade(score) {
    return score >= 90 ? 'A' : score >= 75 ? 'B' : score >= 55 ? 'C' : 'D';
  }

  function weightedBaseMetric(stats, scope, field) {
    const configured = CONFIG.baseWeights[scope] || CONFIG.baseWeights.total;
    const entries = Object.entries(configured).map(([key, baseWeight]) => {
      const stat = stats[key];
      if (!stat) return null;
      let credibility = sampleScore(stat.samples) / 100;
      if (key === 'h2h') credibility *= h2hCredibility(stat.samples);
      return {key, value: stat[field], weight: baseWeight * credibility};
    }).filter((entry) => entry && Number.isFinite(entry.value) && entry.weight > 0);
    const totalWeight = entries.reduce((sum, entry) => sum + entry.weight, 0);
    return totalWeight ? entries.reduce((sum, entry) => sum + entry.value * entry.weight, 0) / totalWeight : null;
  }

  function classification(confidence) {
    return confidence >= 90 ? 'ELITE' : confidence >= 85 ? 'MUITO FORTE' : confidence >= 80 ? 'FORTE' : confidence >= 75 ? 'MODERADA' : 'NÃO RECOMENDAR';
  }

  function calculateValueScore(candidate) {
    // Proxy técnico, não EV: enquanto não há odds, valor nunca participa do
    // Confidence e só desempata candidatos estatisticamente semelhantes.
    const line = Number(candidate.line);
    const minimum = Number(candidate.minimumLine || 0);
    const distance = Number.isFinite(line) ? Math.max(0, line - minimum) : 0;
    return round1(clamp(45 + Math.min(25, distance * 4) + Math.max(0, candidate.adjustedProbability - 75) * .25));
  }

  function evaluateCandidate(input, profileName = 'balanced') {
    const profile = CONFIG.profiles[profileName] || CONFIG.profiles.balanced;
    const predicate = input.predicate || ((value) => value > Number(input.line));
    const stats = {
      h2h: seriesStats(input.bases?.h2h, predicate),
      home: seriesStats(input.bases?.home, predicate),
      away: seriesStats(input.bases?.away, predicate)
    };
    const validStats = Object.values(stats).filter(Boolean);
    const rawProbability = weightedBaseMetric(stats, input.scope || 'total', 'raw');
    const recentScore = weightedBaseMetric(stats, input.scope || 'total', 'recent');
    const probabilities = validStats.filter((item) => item.samples >= 3).map((item) => item.raw);
    const consistency = consistencyScore(probabilities);
    const effectiveSampleScore = weightedBaseMetric(stats, input.scope || 'total', 'samples');
    const sample = sampleScore(effectiveSampleScore || 0);
    const adjustedProbability = rawProbability === null || recentScore === null ? null
      : round1((rawProbability * .68 + recentScore * .32) * (.90 + consistency / 1000));
    const supporting = Number.isFinite(Number(input.supportingScore)) ? clamp(input.supportingScore) : null;
    const dataQuality = dataQualityScore(stats, input.scope || 'total', supporting !== null);
    const supportingForConfidence = supporting === null ? 45 : supporting;
    const components = [
      [adjustedProbability, CONFIG.weights.adjustedProbability], [consistency, CONFIG.weights.consistency],
      [sample, CONFIG.weights.sample], [recentScore, CONFIG.weights.recent], [supportingForConfidence, CONFIG.weights.supporting]
    ].filter(([value]) => value !== null && Number.isFinite(value));
    const componentWeight = components.reduce((sum, [, weight]) => sum + weight, 0);
    let confidence = componentWeight ? components.reduce((sum, [value, weight]) => sum + value * weight, 0) / componentWeight : 0;
    confidence *= .75 + dataQuality / 400;

    const rejectionReasons = [];
    const strengths = [];
    const weaknesses = [];
    const scope = input.scope || 'total';
    const primaryKeys = scope === 'home' ? ['home'] : scope === 'away' ? ['away'] : ['home', 'away'];
    const usablePrimary = primaryKeys.filter((key) => stats[key] && stats[key].samples >= 3);
    if (!validStats.length || !usablePrimary.length) rejectionReasons.push(REJECTION.INSUFFICIENT_DATA);
    if (validStats.every((item) => item.samples < 3)) rejectionReasons.push(REJECTION.LOW_SAMPLE);
    if (scope === 'total' && input.enforcePrimaryFloor !== false) {
      if (stats.home?.samples >= 3 && stats.home.raw < 60) rejectionReasons.push(REJECTION.WEAK_HOME_BASE);
      if (stats.away?.samples >= 3 && stats.away.raw < 60) rejectionReasons.push(REJECTION.WEAK_AWAY_BASE);
    }
    if (probabilities.length > 1 && Math.max(...probabilities) - Math.min(...probabilities) > 50) rejectionReasons.push(REJECTION.HIGH_DIVERGENCE);
    if (rawProbability === null || rawProbability < Math.max(55, profile.minAdjusted - 8)) rejectionReasons.push(REJECTION.LOW_RAW_PROBABILITY);
    if (adjustedProbability === null || adjustedProbability < profile.minAdjusted) rejectionReasons.push(REJECTION.LOW_ADJUSTED_PROBABILITY);
    if (dataQuality < profile.minDataQuality) rejectionReasons.push(REJECTION.LOW_DATA_QUALITY);
    if (supporting !== null && supporting < 45) rejectionReasons.push(REJECTION.WEAK_SUPPORTING_METRICS);

    // Penalidades estruturais nunca viram bônus.
    if (rejectionReasons.includes(REJECTION.HIGH_DIVERGENCE)) confidence -= 12;
    if (rejectionReasons.includes(REJECTION.WEAK_HOME_BASE) || rejectionReasons.includes(REJECTION.WEAK_AWAY_BASE)) confidence -= 10;
    confidence = round1(clamp(confidence));
    if (confidence < profile.minConfidence) rejectionReasons.push(REJECTION.LOW_CONFIDENCE);

    if (adjustedProbability >= 85) strengths.push('Frequência ajustada elevada');
    if (consistency >= 85) strengths.push('Boa consistência entre as bases');
    if (sample >= 75) strengths.push('Amostra com boa sustentação');
    if (supporting !== null && supporting >= 75) strengths.push('Indicadores auxiliares favoráveis');
    if (stats.h2h && stats.h2h.samples < 4) weaknesses.push(`H2H limitado a ${stats.h2h.samples} jogo(s)`);
    if (consistency < 60) weaknesses.push('Bases históricas divergentes');
    if (dataQuality < 75) weaknesses.push('Cobertura de dados limitada');
    [['H2H', stats.h2h], ['Casa', stats.home], ['Fora', stats.away]].forEach(([name, stat]) => {
      if (stat && stat.samples >= 3 && stat.raw >= 75) strengths.push(`${name}: ${stat.hits}/${stat.samples} jogos atingiram a linha`);
      if (stat && stat.samples >= 3 && stat.raw < 60) weaknesses.push(`${name}: somente ${stat.hits}/${stat.samples} jogos atingiram a linha`);
    });

    const candidate = {
      fixtureId: String(input.fixtureId), competitionId: input.competitionId || null, competitionName: input.competitionName || '',
      homeTeam: input.homeTeam || '', awayTeam: input.awayTeam || '', marketType: input.marketType,
      marketGroup: input.marketGroup || input.marketType, scope, line: Number(input.line), label: input.label || '',
      historicalFrequency: round1(rawProbability || 0), rawProbability: round1(rawProbability || 0), adjustedProbability: round1(adjustedProbability || 0),
      h2hProbability: stats.h2h ? round1(stats.h2h.raw) : null, homeProbability: stats.home ? round1(stats.home.raw) : null,
      awayProbability: stats.away ? round1(stats.away.raw) : null, h2hSample: stats.h2h?.samples || 0,
      homeSample: stats.home?.samples || 0, awaySample: stats.away?.samples || 0, recentFormScore: round1(recentScore || 0),
      consistencyScore: consistency, sampleScore: sample, supportingScore: supporting, dataQualityScore: dataQuality,
      dataQualityGrade: qualityGrade(dataQuality), confidenceScore: confidence, classification: classification(confidence),
      strengths, weaknesses, rejectionReasons: [...new Set(rejectionReasons)], dataComplete: dataQuality >= 75,
      status: rejectionReasons.length ? 'REJECTED' : 'APPROVED', bookmakerOdds: input.bookmakerOdds || null,
      impliedProbability: null, expectedValue: null, edge: null, minimumLine: input.minimumLine || 0,
      sourceStats: stats, average: Number.isFinite(Number(input.average)) ? Number(input.average) : null
    };
    candidate.valueScore = calculateValueScore(candidate);
    candidate.finalRank = candidate.confidenceScore;
    return candidate;
  }

  function chooseOptimalLine(candidates, profileName = 'balanced') {
    if (!candidates.length) return null;
    const profile = CONFIG.profiles[profileName] || CONFIG.profiles.balanced;
    const approved = candidates.filter((candidate) => candidate.status === 'APPROVED');
    const pool = approved.length ? approved : candidates;
    const safest = [...pool].sort((a, b) => b.confidenceScore - a.confidenceScore || b.adjustedProbability - a.adjustedProbability)[0];
    if (!safest || profileName === 'conservative') return safest;
    const efficient = pool.filter((candidate) => safest.confidenceScore - candidate.confidenceScore <= profile.maxLineDrop)
      .sort((a, b) => b.confidenceScore - a.confidenceScore || b.adjustedProbability - a.adjustedProbability || b.valueScore - a.valueScore);
    if (profileName === 'value') {
      return efficient.sort((a, b) => (b.confidenceScore + b.valueScore * CONFIG.lineSelection.valueWeight) - (a.confidenceScore + a.valueScore * CONFIG.lineSelection.valueWeight))[0];
    }
    // Equilibrado só sobe a linha quando mantém a confiança próxima da opção
    // mais segura; valor é apenas o último desempate.
    return efficient[0];
  }

  function rankCandidates(candidates) {
    return [...candidates].sort((a, b) =>
      b.confidenceScore - a.confidenceScore || b.adjustedProbability - a.adjustedProbability ||
      b.dataQualityScore - a.dataQualityScore || b.consistencyScore - a.consistencyScore || b.valueScore - a.valueScore
    );
  }

  function correlationPenalty(a, b) {
    if (!a || !b || String(a.fixtureId) !== String(b.fixtureId)) return 0;
    if (a.marketGroup === b.marketGroup) return CONFIG.correlation.sameGroup;
    const pair = new Set([a.marketGroup, b.marketGroup]);
    const nestedPeriodMarket = (first, second) => {
      const full = String(first.marketGroup || '').match(/^(corners|cards|goals|shots|shots_on_target)_(total|home|away)$/);
      const period = String(second.marketGroup || '').match(/^(corners|cards|goals|shots|shots_on_target)_(1h|2h)_(total|home|away)$/);
      return Boolean(full && period && full[1] === period[1] && full[2] === period[3]);
    };
    if (nestedPeriodMarket(a, b) || nestedPeriodMarket(b, a)) return CONFIG.correlation.sameGroup;
    if (pair.has('goals_ft') && pair.has('goal_ht')) return CONFIG.correlation.goalHtWithGoals;
    if ((pair.has('corners_total') && (pair.has('corners_home') || pair.has('corners_away')))) return CONFIG.correlation.totalWithTeamCorners;
    const families = new Set([a.marketGroup, b.marketGroup].map((group) => String(group).replace(/_(total|home|away)$/, '')));
    if (families.has('shots') && families.has('shots_on_target')) return CONFIG.correlation.attempts;
    return 0;
  }

  function marketCategory(candidate) {
    const group = String(candidate?.marketGroup || candidate?.marketType || 'other');
    if (group === 'goals_ft' || group === 'goal_ht' || group.startsWith('goals_')) return 'goals';
    return group.replace(/_(total|home|away)$/, '');
  }

  function diversifyApprovedPool(candidates, requestedCategories) {
    const pool = [...candidates];
    const requested = new Set(requestedCategories || []);
    const structuralRejections = new Set([
      REJECTION.INSUFFICIENT_DATA, REJECTION.LOW_SAMPLE, REJECTION.HIGH_DIVERGENCE,
      REJECTION.LOW_DATA_QUALITY, REJECTION.WEAK_SUPPORTING_METRICS, REJECTION.MANUAL_ONLY
    ]);
    const fixtures = new Set(pool.map((candidate) => String(candidate.fixtureId)));
    fixtures.forEach((fixtureId) => requested.forEach((category) => {
      const fixtureCategory = pool.filter((candidate) => String(candidate.fixtureId) === fixtureId && marketCategory(candidate) === category);
      if (fixtureCategory.some((candidate) => candidate.status === 'APPROVED')) return;
      const fallback = rankCandidates(fixtureCategory.filter((candidate) => {
        if (candidate.status === 'APPROVED') return false;
        if (candidate.rejectionReasons.some((reason) => structuralRejections.has(reason))) return false;
        const primary = candidate.scope === 'home' ? [candidate.sourceStats?.home]
          : candidate.scope === 'away' ? [candidate.sourceStats?.away]
            : [candidate.sourceStats?.home, candidate.sourceStats?.away];
        const validPrimary = primary.filter((item) => item && item.samples >= 3);
        return validPrimary.length === primary.length
          && validPrimary.every((item) => item.raw >= 50)
          && candidate.rawProbability >= 67 && candidate.adjustedProbability >= 65
          && candidate.confidenceScore >= 65 && candidate.dataQualityScore >= 55;
      }))[0];
      if (!fallback) return;
      fallback.status = 'APPROVED';
      fallback.diversityFallback = true;
      fallback.classification = fallback.confidenceScore >= 70 ? 'COMPLEMENTAR FORTE' : 'COMPLEMENTAR';
      fallback.rejectionReasons = [];
      fallback.strengths = [...fallback.strengths, 'Melhor opção consistente disponível nesta categoria'];
    }));
    return pool;
  }

  function buildTicket(candidates, maxGames, profileName = 'balanced', maxSelectionsOverride = null) {
    const configuredProfile = CONFIG.profiles[profileName] || CONFIG.profiles.balanced;
    const profile = {...configuredProfile, maxSelectionsPerFixture: maxSelectionsOverride !== null && maxSelectionsOverride !== undefined && Number.isFinite(Number(maxSelectionsOverride))
      ? Math.max(1, Math.min(6, Number(maxSelectionsOverride))) : configuredProfile.maxSelectionsPerFixture};
    const ranked = rankCandidates(candidates.filter((candidate) => candidate.status === 'APPROVED'));
    const gameLimit = Math.max(1, Number(maxGames) || 1);
    const categoryLimit = Math.max(1, Math.ceil(gameLimit * CONFIG.ticketComposition.maxPrimaryCategoryShare));
    const primary = [];
    const primaryFixtures = new Set();
    const primaryCategories = new Map();
    const approvedCategories = new Map();
    const fixtureCategoryCounts = new Map();
    ranked.forEach((candidate) => {
      const fixtureId = String(candidate.fixtureId);
      if (!fixtureCategoryCounts.has(fixtureId)) fixtureCategoryCounts.set(fixtureId, new Set());
      fixtureCategoryCounts.get(fixtureId).add(marketCategory(candidate));
    });
    const multipleFirst = [...ranked].sort((a, b) =>
      (fixtureCategoryCounts.get(String(b.fixtureId))?.size || 0) - (fixtureCategoryCounts.get(String(a.fixtureId))?.size || 0)
      || b.confidenceScore - a.confidenceScore
    );
    multipleFirst.forEach((candidate) => {
      const category = marketCategory(candidate);
      if (!approvedCategories.has(category)) approvedCategories.set(category, []);
      approvedCategories.get(category).push(candidate);
    });
    // Primeiro garante presença para cada categoria que realmente produziu
    // candidato aprovado. Se duas categorias tiverem como melhor opção o
    // mesmo jogo, procura a próxima partida daquela categoria.
    [...approvedCategories.entries()]
      .sort(([, a], [, b]) => b[0].confidenceScore - a[0].confidenceScore)
      .forEach(([category, categoryCandidates]) => {
        if (primary.length >= gameLimit) return;
        const candidate = categoryCandidates.find((item) => !primaryFixtures.has(item.fixtureId));
        if (!candidate) return;
        primary.push(candidate);
        primaryFixtures.add(candidate.fixtureId);
        primaryCategories.set(category, 1);
      });
    // A lista continua ordenada por confiança, mas nenhuma categoria simples
    // ocupa mais da metade dos jogos enquanto houver outras opções aprovadas.
    multipleFirst.forEach((candidate) => {
      if (primary.length >= gameLimit || primaryFixtures.has(candidate.fixtureId)) return;
      const category = marketCategory(candidate);
      if ((primaryCategories.get(category) || 0) >= categoryLimit) return;
      primary.push(candidate);
      primaryFixtures.add(candidate.fixtureId);
      primaryCategories.set(category, (primaryCategories.get(category) || 0) + 1);
    });
    // Se não houver categorias alternativas suficientes, completa normalmente
    // com as melhores opções restantes, sem reduzir a quantidade solicitada.
    multipleFirst.forEach((candidate) => {
      if (primary.length >= gameLimit || primaryFixtures.has(candidate.fixtureId)) return;
      if (approvedCategories.size > 1 && (primaryCategories.get(marketCategory(candidate)) || 0) >= categoryLimit) return;
      primary.push(candidate);
      primaryFixtures.add(candidate.fixtureId);
      const category = marketCategory(candidate);
      primaryCategories.set(category, (primaryCategories.get(category) || 0) + 1);
    });
    const selected = [...primary];
    primary.forEach((main) => {
      const fixtureSelections = [main];
      const additions = ranked.filter((candidate) => candidate.fixtureId === main.fixtureId && candidate !== main
        && (candidate.diversityFallback || candidate.confidenceScore >= profile.secondMinConfidence)
        && main.confidenceScore - candidate.confidenceScore <= profile.secondMaxGap);
      for (const candidate of additions) {
        if (fixtureSelections.length >= profile.maxSelectionsPerFixture) break;
        if (fixtureSelections.some((existing) => marketCategory(existing) === marketCategory(candidate))) continue;
        if (fixtureSelections.every((existing) => correlationPenalty(existing, candidate) < 60)) {
          fixtureSelections.push(candidate);
          selected.push(candidate);
        }
      }
    });
    return rankCandidates(selected);
  }

  function analysisMetadata(eligibleGames, processedGames, failedGames, profile = 'balanced') {
    const eligible = Math.max(0, Number(eligibleGames) || 0);
    const processed = Math.max(0, Number(processedGames) || 0);
    const failed = Math.max(0, Number(failedGames) || 0);
    return {analysis_complete: processed === eligible && failed === 0, processed_games: processed, eligible_games: eligible, failed_games: failed, profile};
  }

  return {CONFIG, REJECTION, sampleScore, h2hCredibility, seriesStats, consistencyScore, dataQualityScore, qualityGrade,
    evaluateCandidate, chooseOptimalLine, rankCandidates, correlationPenalty, marketCategory, diversifyApprovedPool, buildTicket, analysisMetadata, classification};
});
