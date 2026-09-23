package dpq.core.id;

import dpq.core.model.MessageId;

/** Creates message IDs; injectable so tests can use deterministic IDs (D11a). */
public interface IdGenerator {

    MessageId next(int partition);
}
