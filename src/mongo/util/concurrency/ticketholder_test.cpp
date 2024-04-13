/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "mongo/platform/basic.h"

#include "mongo/db/concurrency/locker_noop_client_observer.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/unittest/barrier.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrency/admission_context.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/tick_source_mock.h"

namespace {
using namespace mongo;

class TicketHolderTest : public ServiceContextTest {
    void setUp() override {
        ServiceContextTest::setUp();

        getServiceContext()->registerClientObserver(std::make_unique<LockerNoopClientObserver>());
        _client = getServiceContext()->makeClient("test");
        _opCtx = _client->makeOperationContext();
    }

protected:
    ServiceContext::UniqueClient _client;
    ServiceContext::UniqueOperationContext _opCtx;
};

template <class H>
void basicTimeout(OperationContext* opCtx) {
    ServiceContext serviceContext;
    serviceContext.setTickSource(std::make_unique<TickSourceMock<Microseconds>>());
    auto mode = TicketHolder::WaitMode::kInterruptible;
    std::unique_ptr<TicketHolder> holder = std::make_unique<H>(1, &serviceContext);
    ASSERT_EQ(holder->used(), 0);
    ASSERT_EQ(holder->available(), 1);
    ASSERT_EQ(holder->outof(), 1);

    AdmissionContext admCtx;
    {
        ScopedTicket ticket(opCtx, holder.get(), mode);
        ASSERT_EQ(holder->used(), 1);
        ASSERT_EQ(holder->available(), 0);
        ASSERT_EQ(holder->outof(), 1);

        ASSERT_FALSE(holder->tryAcquire(&admCtx));
        ASSERT_FALSE(holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now(), mode));
        ASSERT_FALSE(
            holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(1), mode));
        ASSERT_FALSE(
            holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(42), mode));
    }

    ASSERT_EQ(holder->used(), 0);
    ASSERT_EQ(holder->available(), 1);
    ASSERT_EQ(holder->outof(), 1);

    auto ticket = holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now(), mode);
    ASSERT(ticket);
    holder->release(&admCtx, std::move(*ticket));

    ASSERT_EQ(holder->used(), 0);

    ticket = holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(20), mode);
    ASSERT(ticket);
    ASSERT_EQ(holder->used(), 1);

    ASSERT_FALSE(holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(2), mode));
    holder->release(&admCtx, std::move(*ticket));
    ASSERT_EQ(holder->used(), 0);

    //
    // Test resize
    //
    ASSERT(holder->resize(6).isOK());
    ticket = holder->waitForTicket(opCtx, &admCtx, mode);
    ASSERT(ticket);
    ASSERT_EQ(holder->used(), 1);
    ASSERT_EQ(holder->outof(), 6);

    std::array<boost::optional<Ticket>, 5> tickets;
    for (int i = 0; i < 5; ++i) {
        tickets[i] = holder->waitForTicket(opCtx, &admCtx, mode);
        ASSERT_EQ(holder->used(), 2 + i);
        ASSERT_EQ(holder->outof(), 6);
    }

    ASSERT_FALSE(holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(1), mode));

    holder->release(&admCtx, std::move(*ticket));

    ASSERT(holder->resize(5).isOK());
    ASSERT_EQ(holder->used(), 5);
    ASSERT_EQ(holder->outof(), 5);
    ASSERT_FALSE(holder->waitForTicketUntil(opCtx, &admCtx, Date_t::now() + Milliseconds(1), mode));

    for (int i = 0; i < 5; ++i) {
        holder->release(&admCtx, std::move(*tickets[i]));
    }
}

TEST_F(TicketHolderTest, BasicTimeoutFifo) {
    basicTimeout<FifoTicketHolder>(_opCtx.get());
}
TEST_F(TicketHolderTest, BasicTimeoutSemaphore) {
    basicTimeout<SemaphoreTicketHolder>(_opCtx.get());
}

class Stats {
public:
    Stats(TicketHolder* holder) : _holder(holder){};

    long long operator[](StringData field) {
        BSONObjBuilder bob;
        _holder->appendStats(bob);
        auto stats = bob.obj();
        return stats[field].numberLong();
    }

private:
    TicketHolder* _holder;
};

TEST_F(TicketHolderTest, FifoCanceled) {
    ServiceContext serviceContext;
    serviceContext.setTickSource(std::make_unique<TickSourceMock<Microseconds>>());
    auto tickSource = dynamic_cast<TickSourceMock<Microseconds>*>(serviceContext.getTickSource());
    FifoTicketHolder holder(1, &serviceContext);
    Stats stats(&holder);
    AdmissionContext admCtx;

    auto ticket =
        holder.waitForTicket(_opCtx.get(), &admCtx, TicketHolder::WaitMode::kInterruptible);

    stdx::thread waiting([this, &holder]() {
        auto client = this->getServiceContext()->makeClient("waiting");
        auto opCtx = client->makeOperationContext();

        AdmissionContext admCtx;
        auto deadline = Date_t::now() + Milliseconds(100);
        ASSERT_FALSE(holder.waitForTicketUntil(
            opCtx.get(), &admCtx, deadline, TicketHolder::WaitMode::kInterruptible));
    });

    while (holder.queued() == 0) {
        // Wait for thread to take ticket.
    }

    tickSource->advance(Microseconds(100));
    waiting.join();
    holder.release(&admCtx, std::move(ticket));

    ASSERT_EQ(stats["addedToQueue"], 1);
    ASSERT_EQ(stats["removedFromQueue"], 1);
    ASSERT_EQ(stats["queueLength"], 0);
    ASSERT_EQ(stats["totalTimeQueuedMicros"], 100);
    ASSERT_EQ(stats["startedProcessing"], 1);
    ASSERT_EQ(stats["finishedProcessing"], 1);
    ASSERT_EQ(stats["processing"], 0);
    ASSERT_EQ(stats["totalTimeProcessingMicros"], 100);
    ASSERT_EQ(stats["canceled"], 1);
}

DEATH_TEST_F(TicketHolderTest, UnreleasedTicket, "invariant") {
    ServiceContext serviceContext;
    serviceContext.setTickSource(std::make_unique<TickSourceMock<Microseconds>>());
    FifoTicketHolder holder(1, &serviceContext);
    Stats stats(&holder);
    AdmissionContext admCtx;

    auto ticket =
        holder.waitForTicket(_opCtx.get(), &admCtx, TicketHolder::WaitMode::kInterruptible);
}
}  // namespace
