package kafkalite.api;

/**
 * Determines which messages can be read by a fetch or consume request.
 * 
 * <ul>
 *   <li>{@link #LOG_END}: Read all messages up to the end of the log (including uncommitted).
 *       Used by replica fetchers replicating data.</li>
 *   <li>{@link #HIGH_WATERMARK}: Read only messages committed and replicated across ISR
 *       (up to the high watermark). Used by regular consumers to prevent dirty reads.</li>
 * </ul>
 */
public enum FetchIsolation {
    LOG_END,
    HIGH_WATERMARK
}
