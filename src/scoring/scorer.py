from __future__ import annotations

from dataclasses import dataclass

from src.scoring.feature_builder import VisitorFeatures
from src.scoring.presentation import build_explainable_fields
from src.scoring.rules import (
    SCORING_VERSION,
    calculate_rule_contributions,
    normalize_raw_score,
    segment_from_score,
)


@dataclass
class ScoreResult:
    raw_score: int
    normalized_score: float
    segment: str
    explanation: dict[str, int]
    human_explanation: str
    short_reason: str
    recommended_action: str
    scoring_version: str


class RuleBasedScorer:
    def __init__(self, scoring_version: str = SCORING_VERSION) -> None:
        self.scoring_version = scoring_version

    def score(self, features: VisitorFeatures) -> ScoreResult:
        explanation = calculate_rule_contributions(features)
        raw_score = int(sum(explanation.values()))
        normalized_score = round(normalize_raw_score(raw_score), 4)
        segment = segment_from_score(normalized_score)
        explainable = build_explainable_fields(
            features=features,
            explanation=explanation,
            segment=segment,
        )

        return ScoreResult(
            raw_score=raw_score,
            normalized_score=normalized_score,
            segment=segment,
            explanation=explanation,
            human_explanation=str(explainable["human_explanation"]),
            short_reason=str(explainable["short_reason"]),
            recommended_action=str(explainable["recommended_action"]),
            scoring_version=self.scoring_version,
        )
