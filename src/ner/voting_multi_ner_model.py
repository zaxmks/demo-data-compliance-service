from typing import List

import numpy as np
from scipy.sparse import lil_matrix

from src.ner.named_entity import NamedEntity
from src.ner.multi_ner_model import MultiNERModel

class VotingMultiNERModel(MultiNERModel):

    def resolve(self, preds: List, confidence_threshold: float, text: str) -> List:
        """
        Distill final predictions from list of votes
        """
        votes = lil_matrix((len(text), len(preds)), dtype=np.int8)
        for i in range(len(preds)):
            for entity in preds[i]:
                votes[entity.start_char:entity.end_char, i] += np.ones((entity.end_char - entity.start_char, 1))
        final_votes = votes.sum(axis=1) / votes.shape[1]
        final_preds = []
        current = NamedEntity("", 0, 0, "PERSON", 0)
        status = 'O'
        for i in range(final_votes.shape[0]):
            if final_votes[i] > confidence_threshold:
                if status == 'O':
                    status = 'B'
                    current.start_char = i
                else:
                    status = 'I'
            else:
                if status != 'O':
                    current.end_char = i - 1
                    current.text = text[current.start_char:current.end_char + 1]
                    current.confidence = final_votes[current.start_char:current.end_char + 1].mean()
                    final_preds.append(current)
                    current = NamedEntity("", 0, 0, "PERSON", 0)
                    status = 'O'
        return final_preds