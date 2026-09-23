package dpq.core.id;

import dpq.core.model.ReceiptHandle;

/** Creates a fresh receipt handle per delivery; injectable for deterministic tests (D11a). */
public interface ReceiptGenerator {

    ReceiptHandle next();
}
