package seaweedfs.client;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;

/**
 * A tool class for uniformly formatting the keys of Entry's ExtendedMap <br>@date 2024/4/9 10:44<br>
 * Process "Seaweed-" prefix, consistent with http upload file situation
 * <br>
 *
 * <b>Premise:</b>
 * <br>
 * curl -H "Seaweed-name1: value1" -F file=to/path "http://localhost:8888/"
 * <br>
 * When uploading files using Http, you must add the "Seaweed-" prefix to the key of Extended. As shown above,
 * the final storage result is Seaweed-name1, The name of key that actual users understand should be name1
 * <br>
 * The key of Extended is not forced to add the "Seaweed-" prefix when uploading files using FilerClient.
 * This causes inconsistency in the key of Extended format of the two file upload methods.
 * <br>
 * <b>solution:</b>
 * When storing Entry, add the "Seaweed-" prefix to the Extended key,
 * and remove the "Seaweed-" prefix when reading Entry information.
 * Users will not be aware of the "Seaweed-" prefix when using it,
 * and the format of the Extended key in the two upload methods will be unified.
 *
 * @author stillmoon
 */
public class ExtendedFormatUtil {

  public static void addKeyPrefix(FilerProto.Entry.Builder entry) {
    Map<String, ByteString> extendedMap = new HashMap<>(entry.getExtendedCount());
    entry.getExtendedMap().forEach((key, val) -> {
      extendedMap.put("Seaweed-" + key, val);
    });
    entry.clearExtended();
    entry.putAllExtended(extendedMap);
  }

  public static void removeKeyPrefix(FilerProto.Entry.Builder entry) {
    Map<String, ByteString> extendedMap = new HashMap<>(entry.getExtendedCount());
    entry.getExtendedMap().forEach((key, val) -> {
      extendedMap.put(key.replace("Seaweed-", ""), val);
    });
    entry.clearExtended();
    entry.putAllExtended(extendedMap);
  }

}
