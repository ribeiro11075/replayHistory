from typing import Optional
import random
import pandas as pd
import numpy as np


class replayHistory():

    def __init__(self, staffNumber: int, staffCapcity: int, incrementType: Optional[str]=None, increment: float=1, incrementQuitBeforeStart: int=400, incrementQuitAfterStart: int=500, incrementQuitTotal: int=650, duration: int=1000):
        self.incrementTracker = 0
        self.incrementType = incrementType
        self.increment = increment
        self.staffNumber = staffNumber
        self.staffCapcity = staffCapcity
        self.incrementQuitBeforeStart = incrementQuitBeforeStart
        self.incrementQuitAfterStart = incrementQuitAfterStart
        self.incrementQuitTotal = incrementQuitTotal
        self.duration = duration
        self.requiredInputDataFrameColumns = ['actualDuration', 'availableIncrement', 'rank']
        self.staffQueue = {i: {'availableQueue': self.staffCapcity} for i in range(1, self.staffNumber + 1)}
        self.staffQueueColumnNames = ['staff', 'increment', 'availableQueue']


    def _replaceIncrementNaming(self):

        if self.incrementType:
            self.dfProcess.columns = self.dfProcess.columns.str.replace('increment', self.incrementType.lower()).str.replace('Increment', self.incrementType.title())
            self.dfStaffQueue.columns = self.dfStaffQueue.columns.str.replace('increment', self.incrementType.lower()).str.replace('Increment', self.incrementType.title())


    def _checkInputDataFrame(self) -> bool:

        if all([column in self.dfProcess.columns and pd.api.types.is_numeric_dtype(self.dfProcess[column]) for column in self.requiredInputDataFrameColumns]):
            return True

        return False


    def _generateCommonConditions(self) -> (pd.Series, pd.Series, pd.Series, pd.Series):

        hasNotComplete = self.dfProcess['completeIncrement'].isnull()
        hasNotQuitCondition = self.dfProcess['quitIncrement'].isnull()
        hasNotStartedCondition = self.dfProcess['startIncrement'].isnull()
        isAvailableCondition = self.dfProcess['availableIncrement'] <= self.incrementTracker

        return hasNotComplete, hasNotQuitCondition, hasNotStartedCondition, isAvailableCondition


    def _updateTracking(self, df: pd.DataFrame, currentQueueAdd: bool):

        self.dfProcess.update(df) if not df.empty else None

        for staff, count in df['staff'].value_counts().iteritems():
            self.staffQueue[staff]['availableQueue'] -= count if currentQueueAdd else count * -1

    
    def _trackIncrementStartingQueue(self):

        data=[[staff, self.incrementTracker, queue['availableQueue']] for staff, queue in self.staffQueue.items()]
        dfIncrementStart = pd.DataFrame(data=data, columns=self.staffQueueColumnNames)
        self.dfStaffQueue = pd.concat([self.dfStaffQueue, dfIncrementStart])

        return self


    def _quit(self):

        hasNotComplete, hasNotQuitCondition, hasNotStartedCondition, isAvailableCondition = self._generateCommonConditions()
        
        daysSinceAvailable = self.incrementTracker - self.dfProcess['availableIncrement']
        daysAfterStart = self.incrementTracker - self.dfProcess['startIncrement']
        exceedWaitBeforeStartCondition = daysSinceAvailable >= self.incrementQuitBeforeStart
        exceedWaitAfterStartCondition = daysAfterStart >= self.incrementQuitAfterStart
        exceedWaitTotalCondition = daysSinceAvailable >= self.incrementQuitTotal
        quitBeforeStartCondition = hasNotComplete & hasNotQuitCondition & hasNotStartedCondition & exceedWaitBeforeStartCondition
        quitAfterStartCondition = hasNotComplete & hasNotQuitCondition & ~hasNotStartedCondition & exceedWaitAfterStartCondition
        quitTotalTimeCondition = hasNotComplete & hasNotQuitCondition & exceedWaitTotalCondition

        dfQuit = self.dfProcess[quitBeforeStartCondition | quitAfterStartCondition | quitTotalTimeCondition].copy()
        dfQuit['quitIncrement'] = self.incrementTracker

        self._updateTracking(df=dfQuit, currentQueueAdd=False)

        return self


    def _complete(self):

        hasNotComplete, hasNotQuitCondition, hasNotStartedCondition, isAvailableCondition = self._generateCommonConditions()

        durationCompletedCondition = (self.incrementTracker - self.dfProcess['startIncrement']) >= self.dfProcess['actualDuration']
        dfComplete = self.dfProcess[hasNotComplete & hasNotQuitCondition & durationCompletedCondition].copy()
        dfComplete['completeIncrement'] = self.incrementTracker

        self._updateTracking(df=dfComplete, currentQueueAdd=False)

        return self


    def _select(self):

        hasNotComplete, hasNotQuitCondition, hasNotStartedCondition, isAvailableCondition = self._generateCommonConditions()

        staffAvailability = [staff for staff, queueInformation in self.staffQueue.items() for x in range(queueInformation['availableQueue'])]
        random.shuffle(staffAvailability)
        dfTotalPickup = self.dfProcess[hasNotQuitCondition & hasNotStartedCondition & isAvailableCondition].sort_values('rank').iloc[0:len(staffAvailability)].copy()
        staffAssignment = staffAvailability[0:len(dfTotalPickup)]
        dfTotalPickup['startIncrement'] = self.incrementTracker
        dfTotalPickup['staff'] = staffAssignment

        self._updateTracking(df=dfTotalPickup, currentQueueAdd=True)

        return self


    def run(self, dfQueue: pd.DataFrame, saveResults: Optional[bool] = False):

        self.dfProcess = dfQueue.copy()
        self.dfProcess[['startIncrement', 'completeIncrement', 'quitIncrement', 'staff']] = np.nan
        self.dfStaffQueue = pd.DataFrame(columns=self.staffQueueColumnNames)

        if self._checkInputDataFrame():
    
            while self.incrementTracker <= self.duration and not self.dfProcess[self.dfProcess['completeIncrement'].isnull() & self.dfProcess['quitIncrement'].isnull()].empty:
                self._trackIncrementStartingQueue()._quit()._complete()._select()
                self.incrementTracker += self.increment

            self.dfStaffQueue['currentQueue'] = self.staffCapcity - self.dfStaffQueue['availableQueue']
            self._replaceIncrementNaming()

            if saveResults:
                self.dfProcess.to_csv('resultsQueue.csv', index=False)
                self.dfStaffQueue.to_csv('resultsStaff.csv', index=False)


dfQueue = pd.read_csv('historicalData.csv')
simulation = replayHistory(staffNumber=4, staffCapcity=5)
simulation.run(dfQueue=dfQueue, saveResults=True)