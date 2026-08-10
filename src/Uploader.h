#pragma once
// バッチの送信キュー・LittleFS退避・リトライ・バックフィルを担う。
//
// 不変条件: 「2xx が返るまでバッチを捨てない」。失敗理由は区別しない。
// 送信順序: LittleFSの退避ファイル（常に古い）を先に、次にRAMキューの古い順。
//
// 例外: dropOldestWhenFull=true の時だけ、LittleFSも一杯でこれ以上退避できない
// 場面に限り、一番古いデータ（退避ファイルの最古、無ければRAMキューの先頭）を
// 捨てて新しいデータを残す。既定は false でこの例外は発動しない
// （呼び出し側が明示的に選ばない限り不変条件は変わらない）。

#include <Arduino.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>

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
  Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
           uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir,
           bool dropOldestWhenFull = false,
           const char* const* watchResponseHeaders = nullptr,
           const char* const* extraRequestHeaderNames = nullptr,
           const char* const* extraRequestHeaderValues = nullptr,
           const char* caCert = nullptr);

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

  size_t ramQueued() const { return ram_.size(); }
  size_t spillCount() const { return spillCount_; }

  // キュー中で最も古いバッチの開始時刻[us]（送信順序と同じ基準：退避ファイル優先、
  // 無ければRAMキューの先頭）。キューが空なら false。表示で「どれだけ遅れているか」
  // を出す用途を想定（呼び出し側が現在時刻との差を取る）。
  bool oldestQueuedStartUs(uint64_t& outStartUs) const;

  // dropOldestWhenFull=true の時、満杯で捨てた本数の累計。
  // 既定(false)では常に0のまま（捨てないので増えようがない）。
  size_t droppedCount() const { return droppedCount_; }

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
  bool postBatch(const uint8_t* body, size_t len);
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

  std::deque<Batch*> ram_;
  size_t spillCount_ = 0;
  size_t droppedCount_ = 0;
  std::vector<String> lastResponseHeaderValues_;
  uint32_t backoffMs_ = 0;
  uint32_t nextAttemptMs_ = 0;

  // ingest向けのTCP/TLS接続をpostBatch()呼び出しをまたいで使い回す
  // （バックフィルで連続POSTする時のTLSハンドシェイク連発を避けるため）。
  // sendAlert()は宛先ホストが別（alertUrl_）なので、使い回すとホスト切り替えの
  // たびに切断が挟まり意味が無い。専用のローカル接続のまま変えていない。
  WiFiClientSecure client_;
  HTTPClient http_;
};
