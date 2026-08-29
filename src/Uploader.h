#pragma once
// バッチの送信キュー・LittleFS退避・リトライ・バックフィルを担う。
//
// 不変条件: 「2xx が返るまでバッチを捨てない」。失敗理由は区別しない。
// 送信順序: LittleFSの退避ファイル（常に古い）を先に、次にRAMキューの古い順。
//
// 例外1: dropOldestWhenFull=true の時だけ、LittleFSも一杯でこれ以上退避できない
// 場面に限り、一番古いデータ（退避ファイルの最古、無ければRAMキューの先頭）を
// 捨てて新しいデータを残す。既定は false でこの例外は発動しない
// （呼び出し側が明示的に選ばない限り不変条件は変わらない）。
//
// 例外2: discardSpillOn400=true の時だけ、退避ファイルのPOSTがHTTP応答コード
// ちょうど400（サーバが「このボディは壊れている」と明確に判定した場合のみ。
// 403やタイムアウト・切断等コード無しの失敗は含まない）で拒否されたら、その
// 退避ファイルは何度リトライしても成功しないとみなして即座に捨てる
// （NamazuHaUrokoGaNai docs/log/2026-08-11-spill-quarantine-on-400.md
// 「電源断等でLittleFSに0バイト/途中で切れた退避ファイルができ、それが
// 永遠に先頭に居座ってキュー全体が詰まる」実機バグへの対処）。既定は false
// （呼び出し側が明示的に選ばない限り不変条件は変わらない）。
//
// スレッド安全性: enqueue()（受け取り側）とpump()（送信側）を別タスクから
// 同時に呼ぶ用途を想定し、内部にミューテックスを持つ（NamazuHaUrokoGaNai
// docs/log/2026-08-11-uploader-task-split.md、firmwareのuploaderTaskを
// 「吸い出し」「送信」の2タスクに割る変更に対応）。ロックは`ram_`・spillの
// LittleFS操作を囲むだけで、pump()内のネットワークI/O（postBatch()）区間は
// 保持しない——ここを握ったままだと、TLSハンドシェイクが詰まった時に吸い出し側の
// enqueue()まで巻き込んで長時間ブロックし、本末転倒になる（分離した意味が消える）。
// 呼び出し側が複数タスクに分かれない従来の使い方（1タスクが全部呼ぶ）でも、
// ロック自体は無害（競合しないので待たない）なのでそのまま動く。

#include <Arduino.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>

#include <deque>
#include <vector>
#include <cstdint>

#include "Batch.h"

class Uploader {
 public:
  // watchResponseHeaders: 設定すると、バッチPOSTが成功した時にそれらのレスポンス
  // ヘッダの値を保持し lastResponseHeaderValue() で読めるようになる（オプトイン、
  // 既定nullptrで従来通りヘッダを一切見ない）。この層はヘッダを読んで渡すだけで
  // 意味づけは持たない——「何のヘッダか・値をどう解釈するか」は呼び出し側の責務。
  // Electabuzz等の他プロジェクトを巻き込まないための設計（dropOldestWhenFullと
  // 同じ考え方）。**nullptr終端の配列**（末尾に1個nullptrを置く。本数を別引数で
  // 渡す必要はない）。watchResponseHeadersが指す配列はUploaderの寿命の間ずっと
  // 有効でなければならない（コピーせずポインタを保持する。呼び出し側は静的な
  // 配列を渡すこと）。

  // extraRequestHeaderNames/Values: 設定すると、バッチPOSTのたびにそれらの
  // ヘッダをリクエストへ付ける（オプトイン、既定nullptrで従来通り追加しない）。
  // watchResponseHeadersと対称の設計——ここも「決まった名前・値のヘッダを
  // 送るだけ」の汎用APIで、何を送るかの意味づけは呼び出し側の責務。
  // namesは**nullptr終端の配列**、valuesは同じ本数（終端不要）。names[i]の値が
  // values[i]。値を毎回変えたい場合は呼び出し側がvalues配列の指す先を書き換えれば
  // よい（Uploaderはコピーせずポインタを保持する。呼び出し側は静的な配列を渡すこと）。

  // caCert: ingest/alert先（Function URL）を検証するルートCA証明書（PEM文字列）。
  // 渡すとTLS検証にsetCACert()を使う。既定nullptrでは従来通りsetInsecure()
  // （検証なし）にフォールバックする——呼び出し側がまだ証明書を持たない場合に
  // 動かなくならないようにするための互換維持で、推奨は常に渡すこと。
  // 呼び出し側の証明書取得元は問わない（例:
  // firmware/certs/amazon_root_ca1.pem をplatformio.iniのboard_build.embed_txtfiles
  // でリンクする、NamazuHaUrokoGaNaiのOTA取得と同じ手法）。
  // 渡す場合、caCertが指す文字列はUploaderの寿命の間ずっと有効でなければ
  // ならない（コピーせずポインタを保持する。呼び出し側は静的/embedな領域を渡すこと）。
  //
  // maxSpillReadBytes: 指定すると、退避ファイルを読むための領域をbegin()で
  // 一度だけmalloc()し、以後pump()の退避送信のたびに使い回す（既定0は従来通り
  // 読むたびにmalloc()/free()する動作）。狙いはBatchバッファ・TLSの確保/解放と
  // 同じ——同じサイズの確保/解放をヒープ上で繰り返すと、断片化で
  // ESP32のMALLOC_CAP_8BIT側の最大連続空きブロックがじわじわ削れ、いずれ
  // malloc()そのものが失敗し続ける現象を実機で確認した(NamazuHaUrokoGaNai
  // docs/log/2026-08-10-ota-tls-pool-race.md「spill読み込み用mallocの断片化」)。
  // 指定サイズを超える退避ファイルは読めない(0扱いでスキップ)——呼び出し側は
  // 送るバッチの最大サイズ以上を渡すこと。begin()でのmalloc失敗時は従来の
  // 都度malloc/free経路へ安全に縮退する。
  //
  // discardSpillOn400: ファイル冒頭の「不変条件」例外2を参照。既定false。
  //
  // connectTimeoutMs/handshakeTimeoutMs/responseTimeoutMs: postBatch()/sendAlert()の
  // TCP接続確立・TLSハンドシェイク・レスポンスヘッダ読み取りの上限（それぞれ
  // ミリ秒）。既定は3000/3000/3000——呼び出し側のtask watchdogが20秒
  // (`esp_task_wdt_add()`)の環境で、最悪合計12秒（接続タイムアウトの値はハンド
  // シェイク内部のソケットrecv()/send()にも流用されるため、実質「接続1回＋
  // ハンドシェイク判定＋ハンドシェイク内recv一発＋ヘッダ読み取り」の4区間分）に
  // 収まるよう決めた値（NamazuHaUrokoGaNai
  // docs/log/2026-08-29-device2-wdt-timeout-budget-implementation.md）。
  // `Uploader`はこのレポ専用ではなく周波数モニタElectabuzzとも共有しており、
  // 呼び出し側のWDT設定（ひいては安全な合計値）はプロジェクトごとに異なりうる
  // ため引数化した。値を選ぶ時は「3区間の合計＋呼び出し側の他の処理時間」が
  // 自分のtask watchdogの上限を確実に下回るようにすること（DNS解決
  // (`WiFi.hostByName()`)はこの3区間の外側で起きる別枠のブロッキングで、
  // ここでは考慮していない——lwIP既定値に丸投げで別途の対策が要る）。
  Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
           uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir,
           bool dropOldestWhenFull = false,
           const char* const* watchResponseHeaders = nullptr,
           const char* const* extraRequestHeaderNames = nullptr,
           const char* const* extraRequestHeaderValues = nullptr,
           const char* caCert = nullptr, size_t maxSpillReadBytes = 0,
           bool discardSpillOn400 = false, int32_t connectTimeoutMs = 3000,
           uint32_t handshakeTimeoutMs = 3000, uint16_t responseTimeoutMs = 3000);

  ~Uploader();

  // 起動時に LittleFS をマウントし退避ファイル数を数える。
  bool begin();

  // 完成したバッチを送信キューに積む。所有権を受け取る。
  // RAMが一杯なら最も古いRAMバッチをLittleFSへ退避してから積む。
  // 退避先も一杯なら dropOldestWhenFull_ に従う（コンストラクタ参照）。
  void enqueue(Batch* batch);

  // 送信を1歩進める（送信タスクから周期的に呼ぶ）。送れたら true。
  bool pump();

  // 速報を即時POST。失敗は握りつぶす（速報は best-effort）。
  // 本文はプロジェクト固有なので呼び出し側が組む（震度・最大加速度といった
  // 中身をここに置くと、この層が地震計専用になり他プロジェクトと共有できない）。
  bool sendAlert(const char* json, size_t len);

  size_t ramQueued() const;
  size_t spillCount() const;

  // キュー中で最も古いバッチの開始時刻[us]（送信順序と同じ基準：退避ファイル優先、
  // 無ければRAMキューの先頭）。キューが空なら false。表示で「どれだけ遅れているか」
  // を出す用途を想定（呼び出し側が現在時刻との差を取る）。
  bool oldestQueuedStartUs(uint64_t& outStartUs) const;

  // dropOldestWhenFull=true の時、満杯で捨てた本数の累計。
  // 既定(false)では常に0のまま（捨てないので増えようがない）。
  size_t droppedCount() const;

  // RAMキューにある分を全てLittleFSへ退避する。戻り値は退避できた本数。
  // OTA更新など「この後の処理でRAMの内容が失われうる」場面で、再起動前に
  // 呼び出し側が明示的に呼ぶことを想定（enqueue()はRAMが一杯になった時しか
  // 自発的に退避しないため、それ未満で溜まっている分は対象にならない）。
  // 書き込みが途中で失敗したら、そこで止めて残りはRAMに置いたまま返す。
  size_t flushToSpill();

  // headerNameがwatchResponseHeadersに含まれていれば、直近成功したバッチPOSTの
  // レスポンスでのその値（無ければ空文字列）。含まれていなければ常に空文字列。
  // POSTが失敗した回は更新しない（前回成功時の値を保持）。
  String lastResponseHeaderValue(const char* headerName) const;

  // ingest向けの使い回し接続(client_)を明示的に閉じる。pump()は送るものが
  // 無くなった時に自発的にこれを呼ぶが、呼び出し側が「この直後に別のTLS接続を
  // 張る」と分かっている場面（OTA取得の直前など）では、その自発呼び出しを
  // 待たずここで先に閉じておく必要がある——mbedTLSの確保/解放を単一の固定プールへ
  // 隔離する構成（呼び出し側の実装依存。TlsMemPool等）を使っている場合、2本の
  // TLS接続が同時に生きるとプールが単一接続分のサイズ見積もりを超えうるため。
  // 接続が無ければ何もしない。
  void closeConnection();

 private:
  // postBatch()はclient_/http_/lastPostCode_/lastResponseHeaderValues_だけを触る。
  // これらは送信タスク（pump()を呼ぶ側）だけがアクセスする前提のフィールドで
  // ram_/spillと違い複数タスクから触られないため、ロック不要（意図的にmutex_の
  // 外で呼ぶ——ネットワークI/Oをロック区間に含めないため。Uploader.h冒頭の
  // スレッド安全性コメント参照）。
  bool postBatch(const uint8_t* body, size_t len);

  // 以下はram_またはLittleFSのspillディレクトリを直接操作する。呼び出し元が
  // 既にmutex_を保持している前提で書いてある（内部で改めてロックを取らない）。
  // 公開メソッド以外から直接呼ばないこと——lockなしで呼ぶと排他が効かない。
  bool spillOldestRam();               // RAM先頭をファイルへ
  bool loadOldestSpillPath(char* out, size_t outLen, uint64_t& startUs) const;
  void removeSpill(const char* path);
  bool evictOldestSpill();             // 退避ファイルの最古を1本消す（空きを作る）

  const char* ingestUrl_;
  const char* alertUrl_;
  const char* hmacSecret_;
  uint32_t deviceId_;
  uint32_t maxRam_;
  const char* spillDir_;
  bool dropOldestWhenFull_;
  const char* const* watchResponseHeaders_;
  size_t watchResponseHeaderCount_;
  const char* const* extraRequestHeaderNames_;
  const char* const* extraRequestHeaderValues_;
  size_t extraRequestHeaderCount_;
  const char* caCert_;
  bool discardSpillOn400_;

  // ram_・spillCount_・LittleFSのspillディレクトリへのアクセスを排他する。
  // enqueue()（吸い出しタスク）とpump()/flushToSpill()（送信タスク）が別タスクから
  // 同時にこれらを触りうるための保護。取る場所は各メソッドの実装（Uploader.cpp）を
  // 参照——postBatch()のネットワークI/O区間だけは意図的に対象外にしてある。
  SemaphoreHandle_t mutex_ = xSemaphoreCreateMutex();

  std::deque<Batch*> ram_;
  size_t spillCount_ = 0;
  size_t droppedCount_ = 0;
  std::vector<String> lastResponseHeaderValues_;
  uint32_t backoffMs_ = 0;
  uint32_t nextAttemptMs_ = 0;

  // postBatch()の直近呼び出しのHTTP応答コード（未応答/接続失敗等は0のまま）。
  // discardSpillOn400_の判定にだけ使う内部状態。
  int lastPostCode_ = 0;

  // 退避ファイル読み込み用の固定バッファ（begin()で一度だけmalloc()。
  // 未指定/malloc失敗時はspillReadBuf_==nullptrのまま、pump()は従来の
  // 都度malloc/free経路を使う）。
  size_t maxSpillReadBytes_ = 0;
  uint8_t* spillReadBuf_ = nullptr;

  // postBatch()/sendAlert()のTCP接続・TLSハンドシェイク・レスポンスヘッダ読み取り
  // それぞれの上限（コンストラクタ引数参照）。
  int32_t connectTimeoutMs_;
  uint32_t handshakeTimeoutMs_;
  uint16_t responseTimeoutMs_;

  // ingest向けのTCP/TLS接続をpostBatch()呼び出しをまたいで使い回す
  // （バックフィルで連続POSTする時のTLSハンドシェイク連発を避けるため）。
  // sendAlert()は宛先ホストが別（alertUrl_）なので、使い回すとホスト切り替えの
  // たびに切断が挟まり意味が無い。専用のローカル接続のまま変えていない。
  WiFiClientSecure client_;
  HTTPClient http_;
};
